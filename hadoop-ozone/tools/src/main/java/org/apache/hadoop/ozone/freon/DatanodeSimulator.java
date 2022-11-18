package org.apache.hadoop.ozone.freon;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CreatePipelineCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
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
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(
      HddsDatanodeService.class);

  private List<StorageContainerDatanodeProtocolClientSideTranslatorPB>
      scmClients;
  private StorageContainerDatanodeProtocolClientSideTranslatorPB reconClient;

  private ConfigurationSource conf;
  private List<DatanodeState> datanodes;
  private Map<UUID, DatanodeState> datanodesMap;

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
      defaultValue = "false")
  private boolean reload = false;

  private Random random = new Random();

  @Override
  public Void call() throws Exception {
    init();
    if (reload) {
      datanodes = loadDatanodesFromFile();
    } else {
      datanodes = new ArrayList<>(datanodesCount);
    }
    for (int i = datanodes.size(); i < datanodesCount; i++) {
      datanodes.add(new DatanodeState(randomDatanodeDetails(conf)));
    }

    datanodesMap = new HashMap<>();
    for (DatanodeState datanode : datanodes) {
      datanodesMap.put(datanode.datanodeDetails.getUuid(), datanode);
    }

    int successCount = 0;
    for (DatanodeState dn : datanodes) {
      successCount += startDatanode(dn) ? 1 : 0;
    }

    LOGGER.info("{} datanodes was simulated and registered to SCM/Recon",
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

    createContainers();

    return null;
  }

  private void createContainers() throws IOException {
    StorageContainerLocationProtocol
        scmContainerClient = HAUtils.getScmContainerClient(conf);
    int totalAssignedContainers = 0;
    for (DatanodeState datanode : datanodes) {
      totalAssignedContainers += datanode.containers.size();
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
              .containers.put(cp.getContainerInfo().getContainerID(),
                  ContainerReplicaProto.State.OPEN);
          totalAssignedContainers ++;
        }
      }

      totalCreatedContainers++;
      // closed immediately.
      scmContainerClient.closeContainer(cp.getContainerInfo().getContainerID());
    }

    LOGGER.info("Finish assiging {} containers from {} created containers.",
        totalAssignedContainers, totalCreatedContainers);
  }

  private boolean startDatanode(DatanodeState dn)
      throws IOException {
    if (!registerDataNode(dn)) {
      LOGGER.info("Failed to register datanode to SCM: {}",
          dn.datanodeDetails.getUuidString());
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
        scmHeartbeatInterval, TimeUnit.MILLISECONDS);

    LOGGER.info("Successfully registered datanode to SCM: {}",
        dn.datanodeDetails.getUuidString());
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

  List<DatanodeState> loadDatanodesFromFile() {
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
      List<DatanodeState> datanodeStates = (List<DatanodeState>)
          JsonUtils.toJsonList(json, DatanodeState.class);
      LOGGER.info("Loaded {} datanodes from {}", datanodeStates.size(),
          file.getAbsolutePath());
      return datanodeStates;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void heartbeat(StorageContainerDatanodeProtocol client,
                         DatanodeState dn) {
    try {

      SCMHeartbeatRequestProto heartbeat = dn.heartbeatRequest(layoutInfo
      );
      SCMHeartbeatResponseProto response = client.sendHeartbeat(heartbeat);
      dn.ackHeartbeatResponse(response);
    } catch (IOException | TimeoutException e) {
      LOGGER.info("Error sending heartbeat for {}: {}",
          dn.datanodeDetails.getUuidString(), e.getMessage());
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

  private boolean registerDataNode(DatanodeState dn)
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
            client.register(dn.datanodeDetails.getExtendedProtoBufMessage(),
                nodeReport, containerReports, pipelineReports, this.layoutInfo);
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
        LOGGER.error("Error register datanode to SCM", e);
      }
    }

    try {
      reconClient.register(dn.datanodeDetails.getExtendedProtoBufMessage(),
          nodeReport, containerReports, pipelineReports, this.layoutInfo);
    } catch (IOException e) {
      LOGGER.error("Error register datanode to Recon", e);
    }

    dn.isRegistered = isRegistered;

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

  /**
   * Keeping state of a simulated datanode instance.
   * Thread-Unsafe: This is designed to be mutated in the context
   * of a single thread.
   */
  private static class DatanodeState {
    private DatanodeDetails datanodeDetails;
    private boolean isRegistered = false;
    private Instant lastHeartbeat = Instant.MIN;
    private Set<String> pipelines = new HashSet<>();
    private Map<Long, ContainerReplicaProto.State> containers =
        new ConcurrentHashMap<>();

    private DatanodeState(DatanodeDetails datanodeDetails) {
      this.datanodeDetails = datanodeDetails;
    }

    public DatanodeState() {
    }

    public void ackHeartbeatResponse(SCMHeartbeatResponseProto response) {
      for (SCMCommandProto command : response.getCommandsList()) {
        switch (command.getCommandType()) {
        case createPipelineCommand:
          CreatePipelineCommandProto pipelineCmd =
              command.getCreatePipelineCommandProto();
          if (pipelineCmd.getFactor() == HddsProtos.ReplicationFactor.ONE) {
            pipelines.add(pipelineCmd.getPipelineID().getId());
          } else {
            LOGGER.info("Ignored pipeline creation for {}-{}",
                pipelineCmd.getType(), pipelineCmd.getFactor());
          }
          break;
        case closePipelineCommand:
          pipelines.remove(
              command.getClosePipelineCommandProto()
                  .getPipelineID().getId());
          break;
        case closeContainerCommand:
          CloseContainerCommandProto
              closeContainerCmd = command.getCloseContainerCommandProto();
          if (containers.containsKey(closeContainerCmd.getContainerID())) {
            containers.put(closeContainerCmd.getContainerID(),
                ContainerReplicaProto.State.CLOSED);
          } else {
            LOGGER.info("Unrecognized closeContainerCommand");
          }
          break;
        default:
          LOGGER.info("Ignored command: {}", command.getCommandType());
        }
      }
      this.lastHeartbeat = Instant.now();
    }

    public SCMHeartbeatRequestProto heartbeatRequest(
        LayoutVersionProto layoutInfo) throws IOException {
      return
          SCMHeartbeatRequestProto.newBuilder()
              .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
              .setDataNodeLayoutVersion(layoutInfo)
              .setNodeReport(createNodeReport())
              .setPipelineReports(createPipelineReport())
              .setContainerReport(createContainerReport())
              .build();
    }

    private ContainerReportsProto createContainerReport() {
      ContainerReportsProto.Builder builder =
          ContainerReportsProto.newBuilder();
      for (Map.Entry<Long, ContainerReplicaProto.State> entry :
          containers.entrySet()) {
        ContainerReplicaProto container = ContainerReplicaProto.newBuilder()
            .setContainerID(entry.getKey())
            .setReadCount(10_000)
            .setWriteCount(10_000)
            .setReadBytes(10_000_000L)
            .setWriteBytes(5_000_000_000L)
            .setKeyCount(10_000)
            .setUsed(5_000_000_000L)
            .setState(entry.getValue())
            .setBlockCommitSequenceId(1000)
            .setOriginNodeId(datanodeDetails.getUuidString())
            .setReplicaIndex(0)
            .build();
        builder.addReports(container);
      }
      return builder.build();
    }

    private PipelineReportsProto createPipelineReport() {
      PipelineReportsProto.Builder builder = PipelineReportsProto.newBuilder();
      for (String pipelineId : pipelines) {
        builder.addPipelineReport(
            PipelineReport.newBuilder()
                .setPipelineID(HddsProtos.PipelineID
                    .newBuilder().setId(pipelineId).build())
                .setIsLeader(true).build());
      }
      return builder.build();
    }

    private NodeReportProto createNodeReport()
        throws IOException {
      StorageLocationReport storageLocationReport = StorageLocationReport
          .newBuilder()
          .setStorageLocation("/tmp/unreal_storage")
          .setId("simulated-storage-volume")
          .setCapacity((long) StorageUnit.TB.toBytes(10))
          .setScmUsed((long) StorageUnit.TB.toBytes(5))
          .setRemaining((long) StorageUnit.TB.toBytes(5))
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

    @JsonSerialize(using = DatanodeDetailsSerializer.class)
    @JsonDeserialize(using = DatanodeDeserializer.class)
    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public void setDatanodeDetails(
        DatanodeDetails datanodeDetails) {
      this.datanodeDetails = datanodeDetails;
    }

    public Instant getLastHeartbeat() {
      return lastHeartbeat;
    }

    public void setLastHeartbeat(Instant lastHeartbeat) {
      this.lastHeartbeat = lastHeartbeat;
    }

    public Set<String> getPipelines() {
      return pipelines;
    }

    public void setPipelines(Set<String> pipelines) {
      this.pipelines = pipelines;
    }

    public boolean isRegistered() {
      return isRegistered;
    }

    public void setRegistered(boolean registered) {
      isRegistered = registered;
    }

    public Map<Long, ContainerReplicaProto.State> getContainers() {
      return containers;
    }

    public void setContainers(
        Map<Long, ContainerReplicaProto.State> containers) {
      this.containers = containers;
    }
  }

  private static class DatanodeDetailsSerializer
      extends StdSerializer<DatanodeDetails> {
    protected DatanodeDetailsSerializer() {
      super(DatanodeDetails.class);
    }

    @Override
    public void serialize(DatanodeDetails value, JsonGenerator gen,
                          SerializerProvider provider) throws IOException {
      gen.writeBinary(value.getProtoBufMessage().toByteArray());
    }
  }

  private static class DatanodeDeserializer
      extends StdDeserializer<DatanodeDetails> {
    protected DatanodeDeserializer() {
      super(DatanodeDetails.class);
    }

    @Override
    public DatanodeDetails deserialize(JsonParser p,
                                       DeserializationContext ctxt)
        throws IOException {
      byte[] binaryValue = p.getBinaryValue();
      return DatanodeDetails.getFromProtoBuf(
          DatanodeDetailsProto.parseFrom(binaryValue));
    }
  }
}
