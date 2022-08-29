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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.update.client.SCMUpdateServiceGrpcClient;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Wrapper class for Scm protocol clients.
 */
public class ScmClient {

  private final ScmBlockLocationProtocol blockClient;
  private final StorageContainerLocationProtocol containerClient;
  private final LoadingCache<Long, Pipeline> containerLocationCache;
  private SCMUpdateServiceGrpcClient updateServiceGrpcClient;

  ScmClient(ScmBlockLocationProtocol blockClient,
            StorageContainerLocationProtocol containerClient) {
    this(blockClient, containerClient, createContainerLocationCache(containerClient));
  }

  private static LoadingCache<Long, Pipeline> createContainerLocationCache(
      StorageContainerLocationProtocol containerClient) {
    CacheLoader<Long, Pipeline> cacheLoader = new CacheLoader<Long, Pipeline>() {
      @NotNull
      @Override
      public Pipeline load(@NotNull Long containerID) throws Exception {
        ContainerWithPipeline containerWithPipeline =
            containerClient.getContainerWithPipeline(containerID);
        return containerWithPipeline.getPipeline();
      }

      @NotNull
      @Override
      public Map<Long, Pipeline> loadAll(
          @NotNull Iterable<? extends Long> keys) throws Exception {
        List<ContainerWithPipeline> containerWithPipelines =
            containerClient.getContainerWithPipelineBatch(asList(keys));
        return containerWithPipelines.stream().collect(Collectors.toMap(
            x -> x.getContainerInfo().getContainerID(),
            ContainerWithPipeline::getPipeline
        ));
      }
    };

    // LRU cache
    return CacheBuilder.newBuilder()
        .recordStats()
        .maximumSize(1000_000)
        .build(cacheLoader);
  }

  private static <T> List<T> asList(Iterable<? extends T> iterable) {
    if (iterable instanceof List) {
      return (List<T>) iterable;
    }
    return Lists.newLinkedList(iterable);
  }

  ScmClient(ScmBlockLocationProtocol blockClient,
            StorageContainerLocationProtocol containerClient,
            LoadingCache<Long, Pipeline> containerLocationCache) {
    this.containerClient = containerClient;
    this.blockClient = blockClient;
    this.containerLocationCache = containerLocationCache;
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

  public Map<Long, Pipeline> getContainerLocations(
      Collection<Long> containerIDs) throws IOException {
    try {
      return containerLocationCache.getAll(containerIDs);
    } catch (ExecutionException exception) {
      if (exception.getCause() instanceof IOException) {
        throw (IOException) exception.getCause();
      } else {
        throw new IllegalStateException(exception.getCause());
      }
    }
  }
}
