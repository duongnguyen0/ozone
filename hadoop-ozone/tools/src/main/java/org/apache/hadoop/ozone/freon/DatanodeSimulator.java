package org.apache.hadoop.ozone.freon;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsUtils.getReconAddresses;
import static org.apache.hadoop.hdds.HddsUtils.getSCMAddressForDatanodes;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryCount;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryInterval;

@CommandLine.Command(name = "simulate-datanode",
    description =
        "Simulate one or many datanodes and register them to SCM." +
            "This is used to stress test SCM handling a massive cluster.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class DatanodeSimulator implements Callable<Void> {


  private List<StorageContainerDatanodeProtocolClientSideTranslatorPB>
      scmClients;
  private StorageContainerDatanodeProtocolClientSideTranslatorPB reconClient;
  private ConfigurationSource conf;
  private DatanodeState[] datanodes;

  private ScheduledExecutorService heartbeatScheduler;

  @CommandLine.ParentCommand
  private Freon freonCommand;
  @CommandLine.Option(names = {"-t", "--threads"},
      description = "Size of the threadpool running heartbeat.",
      defaultValue = "10")
  private int threadCount = 10;
  @CommandLine.Option(names = {"-n", "--nodes"},
      description = "Number of simulated datanode instances.",
      defaultValue = "1")
  private int datanodesCount = 1;

  private Random random = new Random();

  @Override
  public Void call() throws Exception {
    init();
    datanodes = new DatanodeState[datanodesCount];
    for (int i = 0; i < datanodesCount; i++) {
      datanodes[i] = new DatanodeState(createDatanodeDetails(conf), conf);
    }

    for (DatanodeState dn : datanodes) {
      startDatanode(dn);
    }

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          heartbeatScheduler.shutdown();
          try {
            heartbeatScheduler.awaitTermination(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          scmClients.forEach(IOUtils::closeQuietly);
          IOUtils.closeQuietly(reconClient);
        })
    );

    return null;
  }

  private boolean startDatanode(DatanodeState dn)
      throws IOException {
    System.out.println("Registering datanode to SCM/Recon: "
        + dn.datanodeDetails.getHostName());
    if (!registerDataNode(dn)) {
      return false;
    }

    long scmHeartbeatInterval = HddsServerUtil.getScmHeartbeatInterval(conf);
    for (StorageContainerDatanodeProtocol client : scmClients) {
      // Use random initial delay to add a jitter to avoid peaks.
      long initialDelay = random.nextLong() % scmHeartbeatInterval;
      Runnable runnable = () -> heartBeat(client, dn);
      heartbeatScheduler.scheduleAtFixedRate(runnable, initialDelay,
          scmHeartbeatInterval, TimeUnit.MILLISECONDS);
    }

    long reconHeartbeatInterval =
        HddsServerUtil.getReconHeartbeatInterval(conf);
    long initialDelay = random.nextLong() % reconHeartbeatInterval;
    Runnable runnable = () -> heartBeat(reconClient, dn);
    heartbeatScheduler.scheduleAtFixedRate(runnable, initialDelay,
        scmHeartbeatInterval, TimeUnit.MILLISECONDS);

    return true;
  }

  private void heartBeat(StorageContainerDatanodeProtocol client,
                         DatanodeState dn) {
    try {
      SCMHeartbeatRequestProto heartbeat =
          SCMHeartbeatRequestProto.newBuilder()
              .setDatanodeDetails(dn.datanodeDetails.getProtoBufMessage())
              .setDataNodeLayoutVersion(dn.layoutInfo)
              .setNodeReport(createNodeReport())
              .build();
      SCMHeartbeatResponseProto response = client.sendHeartbeat(heartbeat);
      dn.processCommands(response.getCommandsList());
    } catch (IOException | TimeoutException e) {
      // todo: log
      e.printStackTrace();
    }
  }


  private void init() throws IOException {
    conf = freonCommand.createOzoneConfiguration();
    Collection<InetSocketAddress> addresses = getSCMAddressForDatanodes(conf);
    scmClients = new ArrayList<>(addresses.size());
    for (InetSocketAddress address : addresses) {
      scmClients.add(createScmClient(address));
    }

    InetSocketAddress reconAddress = getReconAddresses(conf);
    reconClient = createReconClient(reconAddress);

    heartbeatScheduler = Executors.newScheduledThreadPool(threadCount);
  }

  private DatanodeDetails createDatanodeDetails(ConfigurationSource conf)
      throws UnknownHostException {

    String hostname = HddsUtils.getHostName(conf) + "-"
        + UUID.randomUUID();

    DatanodeDetails details = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID()).build();
    details.setInitialVersion(DatanodeVersion.CURRENT_VERSION);
    details.setCurrentVersion(DatanodeVersion.CURRENT_VERSION);
    details.setHostName(hostname);
    details.setIpAddress(randomIp());
    details.setVersion(
        HddsVersionInfo.HDDS_VERSION_INFO.getVersion());
    details.setSetupTime(Time.now());
    details.setRevision(
        HddsVersionInfo.HDDS_VERSION_INFO.getRevision());
    details.setBuildDate(HddsVersionInfo.HDDS_VERSION_INFO.getDate());
    details.setCurrentVersion(DatanodeVersion.CURRENT_VERSION);
    return details;
  }

  private boolean registerDataNode(DatanodeState dn)
      throws IOException {
    LayoutVersionProto layoutInfo = dn.layoutInfo;

    ContainerReportsProto containerReports =
        ContainerReportsProto.newBuilder().build();

    NodeReportProto nodeReport = createNodeReport();

    PipelineReportsProto pipelineReports = PipelineReportsProto
        .newBuilder().build();
    boolean isRegistered = false;

    for (StorageContainerDatanodeProtocol client : scmClients) {
      try {
        SCMRegisteredResponseProto response =
            client.register(dn.datanodeDetails.getExtendedProtoBufMessage(),
                nodeReport, containerReports, pipelineReports, layoutInfo);
        if (response.hasHostname() && response.hasIpAddress()) {
          dn.datanodeDetails.setHostName(response.getHostname());
          dn.datanodeDetails.setIpAddress(response.getIpAddress());
        }
        if (response.hasNetworkName() && response.hasNetworkLocation()) {
          dn.datanodeDetails.setNetworkName(response.getNetworkName());
          dn.datanodeDetails.setNetworkLocation(response.getNetworkLocation());
        }
        isRegistered = isRegistered ||
            (response.getErrorCode() ==
                SCMRegisteredResponseProto.ErrorCode.success);
      } catch (IOException e) {
        // todo: log
        e.printStackTrace();
      }
    }

    try {
      SCMRegisteredResponseProto response =
          reconClient.register(dn.datanodeDetails.getExtendedProtoBufMessage(),
              nodeReport, containerReports, pipelineReports, layoutInfo);
    } catch (IOException e) {
      // todo: log
      e.printStackTrace();
    }

    return isRegistered;
  }

  @NotNull
  private NodeReportProto createNodeReport()
      throws IOException {
    long storageCapacity = (long) StorageUnit.TB.toBytes(100);
    StorageLocationReport storageLocationReport = StorageLocationReport
        .newBuilder()
        .setStorageLocation("/tmp/unreal_storage")
        .setId("simulated-storage-volume")
        .setCapacity(storageCapacity)
        .setScmUsed(storageCapacity)
        .setRemaining(0)
        .setStorageType(StorageType.DEFAULT)
        .build();


    StorageLocationReport metaLocationReport = StorageLocationReport
        .newBuilder()
        .setStorageLocation("/tmp/unreal_metadata")
        .setId("simulated-storage-volume")
        .setCapacity((long) StorageUnit.GB.toBytes(100))
        .setScmUsed((long) StorageUnit.GB.toBytes(50))
        .setRemaining((long) StorageUnit.GB.toBytes(50))
        .setStorageType(StorageType.DEFAULT)
        .build();

    return NodeReportProto.newBuilder()
        .addStorageReport(storageLocationReport.getProtoBufMessage())
        .addMetadataStorageReport(
            metaLocationReport.getMetadataProtoBufMessage())
        .build();
  }

  private StorageContainerDatanodeProtocolClientSideTranslatorPB
  createScmClient(InetSocketAddress address) throws IOException {

    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
    RPC.setProtocolEngine(
        hadoopConfig,
        StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    long version =
        RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class);

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
            TimeUnit.MILLISECONDS);

    StorageContainerDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
        StorageContainerDatanodeProtocolPB.class, version,
        address, UserGroupInformation.getCurrentUser(), hadoopConfig,
        NetUtils.getDefaultSocketFactory(hadoopConfig),
        getScmRpcTimeOutInMilliseconds(conf),
        retryPolicy).getProxy();

    return new StorageContainerDatanodeProtocolClientSideTranslatorPB(
        rpcProxy);
  }

  private StorageContainerDatanodeProtocolClientSideTranslatorPB
  createReconClient(InetSocketAddress address) throws IOException {
    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
    RPC.setProtocolEngine(hadoopConfig, ReconDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    long version =
        RPC.getProtocolVersion(ReconDatanodeProtocolPB.class);

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
            TimeUnit.MILLISECONDS);
    ReconDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
        ReconDatanodeProtocolPB.class, version,
        address, UserGroupInformation.getCurrentUser(), hadoopConfig,
        NetUtils.getDefaultSocketFactory(hadoopConfig),
        getScmRpcTimeOutInMilliseconds(conf),
        retryPolicy).getProxy();

    return new StorageContainerDatanodeProtocolClientSideTranslatorPB(rpcProxy);
  }

  private String randomIp() {
    return random.nextInt(256) + "." +
        random.nextInt(256) + "." +
        random.nextInt(256) + "." +
        random.nextInt(256);
  }

  private static int getScmRpcTimeOutInMilliseconds(ConfigurationSource conf) {
    return (int) conf.getTimeDuration(OZONE_SCM_HEARTBEAT_RPC_TIMEOUT,
        OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
  }

  private static class DatanodeState {
    private final DatanodeDetails datanodeDetails;
    private final LayoutVersionProto layoutInfo;

    private DatanodeState(DatanodeDetails datanodeDetails,
                          ConfigurationSource conf) throws IOException {
      this.datanodeDetails = datanodeDetails;

      Storage layoutStorage = new DatanodeLayoutStorage(conf,
          datanodeDetails.getUuidString());

      HDDSLayoutVersionManager layoutVersionManager =
          new HDDSLayoutVersionManager(layoutStorage.getLayoutVersion());

      this.layoutInfo = LayoutVersionProto.newBuilder()
          .setMetadataLayoutVersion(
              layoutVersionManager.getMetadataLayoutVersion())
          .setSoftwareLayoutVersion(
              layoutVersionManager.getSoftwareLayoutVersion())
          .build();
    }

    public void processCommands(List<SCMCommandProto> commands) {
      System.out.println(commands);
    }
  }
}
