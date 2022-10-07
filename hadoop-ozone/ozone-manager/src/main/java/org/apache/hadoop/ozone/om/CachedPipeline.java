package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

class CachedPipeline {
  private final PipelineID id;
  private final ReplicationConfig replicationConfig;

  private final Pipeline.PipelineState state;
  private final List<UUID> nodes;
  private final Map<UUID, Integer> replicaIndexes;
  private final UUID leaderId;
  private Instant creationTimestamp;
  private final UUID suggestedLeaderId;

  public CachedPipeline(Pipeline pipeline) {
    this.id = pipeline.getId();
    this.replicationConfig = pipeline.getReplicationConfig();
    this.state = pipeline.getPipelineState();
    this.leaderId = pipeline.getLeaderId();
    this.creationTimestamp = pipeline.getCreationTimestamp();
    this.suggestedLeaderId = pipeline.getSuggestedLeaderId();
    List<DatanodeDetails> pipelineNodes = pipeline.getNodes();
    this.nodes = pipelineNodes.stream().map(DatanodeDetails::getUuid)
        .collect(Collectors.toList());

    this.replicaIndexes = new HashMap<>();
    for (DatanodeDetails dn : pipelineNodes) {
      int index = pipeline.getReplicaIndex(dn);
      if (index > 0) {
        replicaIndexes.put(dn.getUuid(), index);
      }
    }
  }

  public Pipeline toPipeline(Function<UUID, DatanodeDetails> getDataNode) {
    Pipeline.Builder builder = Pipeline.newBuilder()
        .setId(id)
        .setReplicationConfig(replicationConfig)
        .setState(state)
        .setLeaderId(leaderId)
        .setCreateTimestamp(creationTimestamp.toEpochMilli());
    List<DatanodeDetails> dns = new ArrayList<>(nodes.size());
    Map<DatanodeDetails, Integer> replicaIndex = new LinkedHashMap<>();
    for (UUID node : nodes) {
      DatanodeDetails dn = getDataNode.apply(node);
      dns.add(dn);
      replicaIndex.put(dn, replicaIndexes.get(dn.getUuid()));
    }
    builder.setNodes(dns);
    return builder.build();
  }
}
