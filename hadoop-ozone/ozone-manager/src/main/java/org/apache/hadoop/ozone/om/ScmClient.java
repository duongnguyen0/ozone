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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.update.client.SCMUpdateServiceGrpcClient;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_TTL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_TTL_DEFAULT;

/**
 * Wrapper class for Scm protocol clients.
 */
public class ScmClient {

  private final ScmBlockLocationProtocol blockClient;
  private final StorageContainerLocationProtocol containerClient;
  private final LoadingCache<Long, CachedPipeline> containerLocationCache;
  private final Cache<UUID, DatanodeDetails> datanodeCache;

  private SCMUpdateServiceGrpcClient updateServiceGrpcClient;

  ScmClient(ScmBlockLocationProtocol blockClient,
            StorageContainerLocationProtocol containerClient,
            OzoneConfiguration configuration) {
    this.containerClient = containerClient;
    this.blockClient = blockClient;
    // todo: to be solved, eviction policy.
    this.datanodeCache = CacheBuilder.newBuilder().build();
    this.containerLocationCache =
        createContainerLocationCache(configuration, containerClient, datanodeCache);
  }

  private static LoadingCache<Long, CachedPipeline> createContainerLocationCache(
      final OzoneConfiguration configuration,
      final StorageContainerLocationProtocol containerClient,
      final Cache<UUID, DatanodeDetails> datanodeCache) {
    int maxSize = configuration.getInt(OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE,
        OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE_DEFAULT);
    TimeUnit unit =  OZONE_OM_CONTAINER_LOCATION_CACHE_TTL_DEFAULT.getUnit();
    long ttl = configuration.getTimeDuration(
        OZONE_OM_CONTAINER_LOCATION_CACHE_TTL,
        OZONE_OM_CONTAINER_LOCATION_CACHE_TTL_DEFAULT.getDuration(), unit);
    return CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(ttl, unit)
        .build(new CacheLoader<Long, CachedPipeline>() {
          @NotNull
          @Override
          public CachedPipeline load(@NotNull Long key) throws Exception {
            Pipeline p =
                containerClient.getContainerWithPipeline(key).getPipeline();
            collectDatanodes(p);
            return new CachedPipeline(p);
          }

          private void collectDatanodes(Pipeline p) {
            p.getNodes().forEach(x -> datanodeCache.put(x.getUuid(), x));
          }

          @NotNull
          @Override
          public Map<Long, CachedPipeline> loadAll(
              @NotNull Iterable<? extends Long> keys) throws Exception {
            List<ContainerWithPipeline> pipelines =
                containerClient.getContainerWithPipelineBatch(keys);
            pipelines.forEach(x -> collectDatanodes(x.getPipeline()));
            return pipelines.stream()
                .collect(Collectors.toMap(
                    x -> x.getContainerInfo().getContainerID(),
                    x -> new CachedPipeline(x.getPipeline())
                ));
          }
        });
  }

  public ScmBlockLocationProtocol getBlockClient() {
    return this.blockClient;
  }

  public StorageContainerLocationProtocol getContainerClient() {
    return this.containerClient;
  }

  public void setUpdateServiceGrpcClient(
      SCMUpdateServiceGrpcClient updateClient) {
    this.updateServiceGrpcClient = updateClient;
  }

  public SCMUpdateServiceGrpcClient getUpdateServiceGrpcClient() {
    return updateServiceGrpcClient;
  }

  public Map<Long, Pipeline> getContainerLocations(Iterable<Long> containerIds,
                                                  boolean forceRefresh)
      throws IOException {
    if (forceRefresh) {
      containerLocationCache.invalidateAll(containerIds);
    }
    try {
      Map<Long, CachedPipeline> all =
          containerLocationCache.getAll(containerIds);
      Map<Long, Pipeline> result = new LinkedHashMap<>();
      for (Map.Entry<Long, CachedPipeline> entry : all.entrySet()) {
        Pipeline p = entry.getValue().toPipeline(datanodeCache::getIfPresent);
      }
      return result;
    } catch (ExecutionException e) {
      return handleCacheExecutionException(e);
    }
  }

  private <T> T handleCacheExecutionException(ExecutionException e)
      throws IOException {
    if (e.getCause() instanceof IOException) {
      throw (IOException) e.getCause();
    }
    throw new IllegalStateException("Unexpected exception accessing " +
        "container location", e.getCause());
  }
}
