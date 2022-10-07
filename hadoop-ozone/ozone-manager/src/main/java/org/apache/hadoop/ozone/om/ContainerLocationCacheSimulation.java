/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.hadoop.hdds.client.ReplicationConfig.fromTypeAndFactor;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.RATIS;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.REPLICATION;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.REST;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.STANDALONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Simple simulation that poplulates a pre-determined number, e.g 100K, of
 * container locations to the cache in {@link ScmClient}.
 *
 * This allows external analysis to explore the cache memory footprint.
 */
public class ContainerLocationCacheSimulation {
  private static DatanodeDetails randomDatanode() {
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName(randomAlphabetic(50))
        .setIpAddress(randomAlphabetic(15)) // IPV4 (xxx.xxx.xxx.xxx)
        .addPort(new Port(RATIS, 1024))
        .addPort(new Port(STANDALONE, 1025))
        .addPort(new Port(REST, 1026))
        .addPort(new Port(REPLICATION, 1025))
        .setNetworkName(randomAlphabetic(20))
        .setNetworkLocation("/" + randomAlphabetic(19))
        .setPersistedOpState(IN_SERVICE)
        .build();
  }

  private static ScmClient scmClient;
  public static void main(String[] args) throws IOException {
    int numberOfDataNodes = 10_000;
    MockStorageContainerLocationProtocol mockLocationProtocol =
        new MockStorageContainerLocationProtocol(numberOfDataNodes);
    int numberOfItems = 1000_000;

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE, numberOfItems + 10000);

    scmClient = new ScmClient(null, mockLocationProtocol, conf);

    for (int i = 0; i < numberOfItems; i++) {
      scmClient.getContainerLocations(Collections.singleton((long) i), false);
    }
    mockLocationProtocol.datanodeDetails = null;
    System.out.println("Populated " + numberOfItems + " to the cache.");

    for (int i = 0; i < numberOfItems; i++) {
      scmClient.getContainerLocations(Collections.singleton((long) i), false);
    }

    System.gc();
    System.out.println("Ready to examine heap size");

    // Pause to allow external operation, e.g. heapdump export
    System.in.read();
  }

  public static class MockStorageContainerLocationProtocol implements
      StorageContainerLocationProtocol {

    List<DatanodeDetails> datanodeDetails;
    Random rand = new Random();

    MockStorageContainerLocationProtocol(int numberOfDataNodes) {
      datanodeDetails = new ArrayList<>(numberOfDataNodes);
      for (int i = 0; i < numberOfDataNodes; i++) {
        datanodeDetails.add(randomDatanode());
      }
    }

    DatanodeDetails getRandomDatanode() {
      return datanodeDetails.get(rand.nextInt(datanodeDetails.size()));
    }

    @Override
    public ContainerWithPipeline allocateContainer(
        HddsProtos.ReplicationType replicationType,
        HddsProtos.ReplicationFactor factor, String owner) throws IOException {
      return null;
    }

    @Override
    public ContainerInfo getContainer(long containerID) throws IOException {
      return null;
    }

    @Override
    public ContainerWithPipeline getContainerWithPipeline(long containerID)
        throws IOException {
      return ContainerWithPipeline.fromProtobuf(
          randomPipeline(containerID).getProtobuf(3)
      );
    }

    private ContainerWithPipeline randomPipeline(long containerId) {
      ReplicationConfig replicationConf = fromTypeAndFactor(
          ReplicationType.RATIS, ReplicationFactor.THREE);
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerID(containerId)
          .setState(HddsProtos.LifeCycleState.DELETING)
          .setOwner("")
          .setStateEnterTime(0)
          .setReplicationConfig(replicationConf)
          .build();
      Pipeline pipeline = Pipeline.newBuilder()
          .setId(PipelineID.randomId())
          .setNodes(asList(
              getRandomDatanode(),
              getRandomDatanode(),
              getRandomDatanode())
          )
          .setReplicationConfig(replicationConf)
          .setState(Pipeline.PipelineState.OPEN)
          .build();
      return new ContainerWithPipeline(containerInfo, pipeline);
    }


    @Override
    public List<HddsProtos.SCMContainerReplicaProto> getContainerReplicas(
        long containerId, int clientVersion) throws IOException {
      return null;
    }

    @Override
    public List<ContainerWithPipeline> getContainerWithPipelineBatch(
        Iterable<? extends Long> containerIDs) throws IOException {
      List<ContainerWithPipeline> list = new LinkedList<>();
      for (Long containerID : containerIDs) {
        ContainerWithPipeline containerWithPipeline =
            getContainerWithPipeline(containerID);
        list.add(containerWithPipeline);
      }
      return list;
    }

    @Override
    public List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
        List<Long> containerIDs) {
      return null;
    }

    @Override
    public List<ContainerInfo> listContainer(long startContainerID, int count)
        throws IOException {
      return null;
    }

    @Override
    public List<ContainerInfo> listContainer(long startContainerID, int count,
                                             HddsProtos.LifeCycleState state)
        throws IOException {
      return null;
    }

    @Override
    public List<ContainerInfo> listContainer(long startContainerID, int count,
                                             HddsProtos.LifeCycleState state,
                                             HddsProtos.ReplicationFactor factor)
        throws IOException {
      return null;
    }

    @Override
    public List<ContainerInfo> listContainer(long startContainerID, int count,
                                             HddsProtos.LifeCycleState state,
                                             HddsProtos.ReplicationType replicationType,
                                             ReplicationConfig replicationConfig)
        throws IOException {
      return null;
    }

    @Override
    public void deleteContainer(long containerID) throws IOException {

    }

    @Override
    public List<HddsProtos.Node> queryNode(
        HddsProtos.NodeOperationalState opState, HddsProtos.NodeState state,
        HddsProtos.QueryScope queryScope, String poolName, int clientVersion)
        throws IOException {
      return null;
    }

    @Override
    public List<DatanodeAdminError> decommissionNodes(List<String> nodes)
        throws IOException {
      return null;
    }

    @Override
    public List<DatanodeAdminError> recommissionNodes(List<String> nodes)
        throws IOException {
      return null;
    }

    @Override
    public List<DatanodeAdminError> startMaintenanceNodes(List<String> nodes,
                                                          int endInHours)
        throws IOException {
      return null;
    }

    @Override
    public void closeContainer(long containerID) throws IOException {

    }

    @Override
    public Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
                                              HddsProtos.ReplicationFactor factor,
                                              HddsProtos.NodePool nodePool)
        throws IOException {
      return null;
    }

    @Override
    public List<Pipeline> listPipelines() throws IOException {
      return null;
    }

    @Override
    public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
        throws IOException {
      return null;
    }

    @Override
    public void activatePipeline(HddsProtos.PipelineID pipelineID)
        throws IOException {

    }

    @Override
    public void deactivatePipeline(HddsProtos.PipelineID pipelineID)
        throws IOException {

    }

    @Override
    public void closePipeline(HddsProtos.PipelineID pipelineID)
        throws IOException {

    }

    @Override
    public ScmInfo getScmInfo() throws IOException {
      return null;
    }

    @Override
    public int resetDeletedBlockRetryCount(List<Long> txIDs)
        throws IOException {
      return 0;
    }

    @Override
    public boolean inSafeMode() throws IOException {
      return false;
    }

    @Override
    public Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
        throws IOException {
      return null;
    }

    @Override
    public boolean forceExitSafeMode() throws IOException {
      return false;
    }

    @Override
    public void startReplicationManager() throws IOException {

    }

    @Override
    public void stopReplicationManager() throws IOException {

    }

    @Override
    public boolean getReplicationManagerStatus() throws IOException {
      return false;
    }

    @Override
    public ReplicationManagerReport getReplicationManagerReport()
        throws IOException {
      return null;
    }

    @Override
    public StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto startContainerBalancer(
        Optional<Double> threshold, Optional<Integer> iterations,
        Optional<Integer> maxDatanodesPercentageToInvolvePerIteration,
        Optional<Long> maxSizeToMovePerIterationInGB,
        Optional<Long> maxSizeEnteringTargetInGB,
        Optional<Long> maxSizeLeavingSourceInGB) throws IOException {
      return null;
    }

    @Override
    public void stopContainerBalancer() throws IOException {

    }

    @Override
    public boolean getContainerBalancerStatus() throws IOException {
      return false;
    }

    @Override
    public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
        String ipaddress, String uuid, int clientVersion) throws IOException {
      return null;
    }

    @Override
    public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
        boolean mostUsed, int count, int clientVersion) throws IOException {
      return null;
    }

    @Override
    public UpgradeFinalizer.StatusAndMessages finalizeScmUpgrade(
        String upgradeClientID) throws IOException {
      return null;
    }

    @Override
    public UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(
        String upgradeClientID, boolean force, boolean readonly)
        throws IOException {
      return null;
    }

    @Override
    public Token<?> getContainerToken(ContainerID containerID)
        throws IOException {
      return null;
    }

    @Override
    public long getContainerCount() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }
  }
}
