package org.apache.hadoop.ozone.freon;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HAUtils;
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
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdds.HddsUtils.getReconAddresses;
import static org.apache.hadoop.hdds.HddsUtils.getSCMAddressForDatanodes;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmHeartbeatInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryCount;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryInterval;

/**
 * This command simulates a number of datanodes and target to coordinate with
 * SCM to create a number of containers on the said datanodes.
 *
 * This tool is created to verify the SCM/Recon ability to handle thousands
 * datanodes and exabytes of data.
 *
 * Usage:
 *    ozone freon simulate-datanode -t 20 -n 5000 -c 10000
 *      -t: number of threads to run datanodes heartbeat.
 *      -n: number of data node to simulate.
 *      -c: number containers to simulate per datanode.
 *
 * The simulation can be stopped and restored safely as datanode states,
 *  including pipelines and containers are saved to a file when the process
 *  exits.
 *
 * When the number containers exceeds the required one, all datanodes are
 * transitioned to readonly mode (all pipelines are closed).
 */
@CommandLine.Command(name = "simulate-datanode",
    description =
        "Simulate one or many datanodes and register them to SCM." +
            "This is used to stress test SCM handling a massive cluster.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class DatanodeSimulator implements Callable<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      DatanodeSimulator.class);

  private List<StorageContainerDatanodeProtocolClientSideTranslatorPB>
      scmClients;
  private StorageContainerDatanodeProtocolClientSideTranslatorPB reconClient;

  private ConfigurationSource conf;
  private List<DatanodeSimulationState> datanodes;
  private Map<UUID, DatanodeSimulationState> datanodesMap;

  private ScheduledExecutorService heartbeatScheduler;
  private LayoutVersionProto layoutInfo;

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

  @CommandLine.Option(names = {"-c", "--containers"},
      description = "Number of simulated containers per datanode.",
      defaultValue = "5")
  private int containers = 1;

  @CommandLine.Option(names = {"-r", "--reload"},
      description = "Reload the datanodes created by previous simulation run.",
      defaultValue = "true")
  private boolean reload = true;

  private Random random = new Random();

  // stats
  private AtomicLong totalHeartbeats = new AtomicLong(0);
  private AtomicLong totalReportedContainers = new AtomicLong(0);
  private StorageContainerLocationProtocol scmContainerClient;

  @Override
  public Void call() throws Exception {
    init();
    loadOrCreateDatanodes();

    // Register datanodes to SCM/Recon and schedule heartbeat for each.
    int successCount = 0;
    for (DatanodeSimulationState dn : datanodes) {
      successCount += startDatanode(dn) ? 1 : 0;
    }

    LOGGER.info("{} datanodes have been created and registered to SCM/Recon",
        successCount);

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
          LOGGER.info("Successfully closed all the used resources");
          storeDataNodesToFile();
        })
    );

    backgroundStatsReporter();

    try {
      growContainers();
    } catch (IOException e) {
      LOGGER.error("Error creating containers, exiting.", e);
      throw e;
    }

    LOGGER.info("Finished creating container, " +
        "transitioning datanodes to readonly");
    moveDatanodesToReadonly();

    LOGGER.info("All datanodes have been transitioned to read-only.");

    return null;
  }

  private void moveDatanodesToReadonly() {
    for (DatanodeSimulationState dn : datanodes) {
      dn.setReadOnly(true);
      for (String pipeline : dn.getPipelines()) {
        try {
          scmContainerClient.closePipeline(HddsProtos.PipelineID.newBuilder()
              .setId(pipeline).build());
        } catch (IOException e) {
          LOGGER.error("Error closing pipeline {}", pipeline, e);
        }
      }
    }
  }

  private void backgroundStatsReporter() {
    long interval = getScmHeartbeatInterval(conf);
    final AtomicLong lastTotalHeartbeats = new AtomicLong(0);
    final AtomicLong lastTotalReportedContainers = new AtomicLong(0);
    final AtomicReference<Instant> lastCheck =
        new AtomicReference<>(Instant.now());
    heartbeatScheduler.scheduleAtFixedRate(() -> {
      long heartbeats = totalHeartbeats.get() - lastTotalHeartbeats.get();
      lastTotalHeartbeats.set(totalHeartbeats.get());
      long reportedContainers = totalReportedContainers.get() -
          lastTotalReportedContainers.get();
      lastTotalReportedContainers.set(totalReportedContainers.get());

      long intervalInSeconds = Instant.now().getEpochSecond()
          - lastCheck.get().getEpochSecond();
      lastCheck.set(Instant.now());

      LOGGER.info("Heartbeat status: \n" +
              "Total heartbeat in cycle: {} ({} per second) \n" +
              "Total container reported in cycle: {} ({} per second)",
          heartbeats, heartbeats / intervalInSeconds,
          reportedContainers, reportedContainers / intervalInSeconds);
    }, interval, interval, TimeUnit.MILLISECONDS);
  }

  private void loadOrCreateDatanodes() throws UnknownHostException {
    if (reload) {
      datanodes = loadDatanodesFromFile();
    } else {
      datanodes = new ArrayList<>(datanodesCount);
    }
    for (int i = datanodes.size(); i < datanodesCount; i++) {
      datanodes.add(new DatanodeSimulationState(randomDatanodeDetails(conf)));
    }

    datanodesMap = new HashMap<>();
    for (DatanodeSimulationState datanode : datanodes) {
      datanodesMap.put(datanode.getDatanodeDetails().getUuid(), datanode);
    }
  }

  private void growContainers() throws IOException {
    int totalAssignedContainers = 0;
    for (DatanodeSimulationState datanode : datanodes) {
      totalAssignedContainers += datanode.getContainers().size();
    }
    int totalExpectedContainers = datanodesCount * containers;
    int totalCreatedContainers = 0;
    LOGGER.info("Start to create containers.");
    while (totalAssignedContainers < totalExpectedContainers) {
      ContainerWithPipeline cp =
          scmContainerClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, "test");

      for (DatanodeDetails datanode : cp.getPipeline().getNodeSet()) {
        if (datanodesMap.containsKey(datanode.getUuid())) {
          datanodesMap.get(datanode.getUuid())
              .getContainers().put(cp.getContainerInfo().getContainerID(),
                  ContainerReplicaProto.State.OPEN);
          totalAssignedContainers++;
        }
      }

      totalCreatedContainers++;
      // closed immediately.
      scmContainerClient.closeContainer(cp.getContainerInfo().getContainerID());
    }

    LOGGER.info("Finish assigning {} containers from {} created containers.",
        totalAssignedContainers, totalCreatedContainers);
  }

  private boolean startDatanode(DatanodeSimulationState dn)
      throws IOException {
    if (!registerDataNode(dn)) {
      LOGGER.info("Failed to register datanode to SCM: {}",
          dn.getDatanodeDetails().getUuidString());
      return false;
    }

    // Schedule heartbeat tasks for the given datanode to all SCMs/Recon.
    long scmHeartbeatInterval = HddsServerUtil.getScmHeartbeatInterval(conf);
    for (StorageContainerDatanodeProtocol client : scmClients) {
      // Use random initial delay as a jitter to avoid peaks.
      long initialDelay = random.nextLong() % scmHeartbeatInterval;
      Runnable runnable = () -> heartbeat(client, dn);
      heartbeatScheduler.scheduleAtFixedRate(runnable, initialDelay,
          scmHeartbeatInterval, TimeUnit.MILLISECONDS);
    }

    long reconHeartbeatInterval =
        HddsServerUtil.getReconHeartbeatInterval(conf);
    long initialDelay = random.nextLong() % reconHeartbeatInterval;
    Runnable runnable = () -> heartbeat(reconClient, dn);
    heartbeatScheduler.scheduleAtFixedRate(runnable, initialDelay,
        reconHeartbeatInterval, TimeUnit.MILLISECONDS);

    LOGGER.info("Successfully registered datanode to SCM: {}",
        dn.getDatanodeDetails().getUuidString());
    return true;
  }

  void storeDataNodesToFile() {
    File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
    File file = new File(metaDirPath, "datanode-simulation.json");
    try (OutputStream os = Files.newOutputStream(file.toPath())) {
      IOUtils.write(JsonUtils.toJsonString(datanodes), os,
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOGGER.info("{} datanodes has been saved to {}", datanodes.size(),
        file.getAbsolutePath());
  }

  List<DatanodeSimulationState> loadDatanodesFromFile() {
    File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
    File file = new File(metaDirPath, "datanode-simulation.json");
    if (!file.exists()) {
      LOGGER.info("File {} doesn't exists, nothing is loaded",
          file.getAbsolutePath());
      return new ArrayList<>();
    }
    try {
      String json =
          new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
      List<DatanodeSimulationState> datanodeStates =
          (List<DatanodeSimulationState>)
              JsonUtils.toJsonList(json, DatanodeSimulationState.class);
      LOGGER.info("Loaded {} datanodes from {}", datanodeStates.size(),
          file.getAbsolutePath());
      return datanodeStates;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void heartbeat(StorageContainerDatanodeProtocol client,
                         DatanodeSimulationState dn) {
    try {
      SCMHeartbeatRequestProto heartbeat = dn.heartbeatRequest(layoutInfo);
      SCMHeartbeatResponseProto response = client.sendHeartbeat(heartbeat);
      dn.ackHeartbeatResponse(response);
      totalHeartbeats.incrementAndGet();
      totalReportedContainers.addAndGet(heartbeat
          .getContainerReport().getReportsCount());
    } catch (Exception e) {
      LOGGER.info("Error sending heartbeat for {}: {}",
          dn.getDatanodeDetails().getUuidString(), e.getMessage());
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

    scmContainerClient = HAUtils.getScmContainerClient(conf);

    this.layoutInfo = createLayoutInfo();
  }

  private LayoutVersionProto createLayoutInfo() throws IOException {
    Storage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString());

    HDDSLayoutVersionManager layoutVersionManager =
        new HDDSLayoutVersionManager(layoutStorage.getLayoutVersion());

    return LayoutVersionProto.newBuilder()
        .setMetadataLayoutVersion(
            layoutVersionManager.getMetadataLayoutVersion())
        .setSoftwareLayoutVersion(
            layoutVersionManager.getSoftwareLayoutVersion())
        .build();
  }

  private DatanodeDetails randomDatanodeDetails(ConfigurationSource conf)
      throws UnknownHostException {
    DatanodeDetails details = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .build();
    details.setInitialVersion(DatanodeVersion.CURRENT_VERSION);
    details.setCurrentVersion(DatanodeVersion.CURRENT_VERSION);
    details.setHostName(HddsUtils.getHostName(conf));
    details.setIpAddress(randomIp());
    details.setPort(DatanodeDetails.Port.Name.STANDALONE, 0);
    details.setPort(DatanodeDetails.Port.Name.RATIS, 0);
    details.setPort(DatanodeDetails.Port.Name.REST, 0);
    details.setVersion(
        HddsVersionInfo.HDDS_VERSION_INFO.getVersion());
    details.setSetupTime(Time.now());
    details.setRevision(
        HddsVersionInfo.HDDS_VERSION_INFO.getRevision());
    details.setBuildDate(HddsVersionInfo.HDDS_VERSION_INFO.getDate());
    details.setCurrentVersion(DatanodeVersion.CURRENT_VERSION);
    return details;
  }

  private boolean registerDataNode(DatanodeSimulationState dn)
      throws IOException {

    ContainerReportsProto containerReports =
        ContainerReportsProto.newBuilder().build();

    NodeReportProto nodeReport = dn.createNodeReport();

    PipelineReportsProto pipelineReports = PipelineReportsProto
        .newBuilder().build();
    boolean isRegistered = false;

    for (StorageContainerDatanodeProtocol client : scmClients) {
      try {
        SCMRegisteredResponseProto response =
            client.register(
                dn.getDatanodeDetails().getExtendedProtoBufMessage(),
                nodeReport, containerReports, pipelineReports, this.layoutInfo);
        if (response.hasHostname() && response.hasIpAddress()) {
          dn.getDatanodeDetails().setHostName(response.getHostname());
          dn.getDatanodeDetails().setIpAddress(response.getIpAddress());
        }
        if (response.hasNetworkName() && response.hasNetworkLocation()) {
          dn.getDatanodeDetails().setNetworkName(response.getNetworkName());
          dn.getDatanodeDetails()
              .setNetworkLocation(response.getNetworkLocation());
        }
        isRegistered = isRegistered ||
            (response.getErrorCode() ==
                SCMRegisteredResponseProto.ErrorCode.success);
      } catch (IOException e) {
        LOGGER.error("Error register datanode to SCM", e);
      }
    }

    try {
      reconClient.register(dn.getDatanodeDetails().getExtendedProtoBufMessage(),
          nodeReport, containerReports, pipelineReports, this.layoutInfo);
    } catch (IOException e) {
      LOGGER.error("Error register datanode to Recon", e);
    }

    dn.setRegistered(isRegistered);

    return isRegistered;
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

}
