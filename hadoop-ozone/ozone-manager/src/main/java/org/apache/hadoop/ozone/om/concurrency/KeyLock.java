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
package org.apache.hadoop.ozone.om.concurrency;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.OzoneManagerUtils.getBucketLayout;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;

/**
 * Handle locking logic foe key ops in OBS, FSO and Legacy buckets.
 */
public class KeyLock {
  static LockHolder acquireWriteLock(OMMetadataManager metadataManager,
                                     KeyArgs keyArgs)
      throws IOException {
    return acquireWriteLock(metadataManager, keyArgs.getVolumeName(),
        keyArgs.getBucketName(), keyArgs.getKeyName());
  }

  static LockHolder acquireWriteLock(OMMetadataManager metadataManager,
                                     String volume, String bucket,
                                     String... keys)
      throws IOException {
    BucketLayout layout = getBucketLayout(metadataManager, volume, bucket);
    if (layout == OBJECT_STORE) {
      return ObsKeyLock.acquireObsWriteLock(metadataManager.getLock(),
          volume, bucket, keys);
    } else if (layout == FILE_SYSTEM_OPTIMIZED) {
      return FsoKeyLock.acquireFsoWriteLock(metadataManager.getLock(), volume, bucket,
          keys);
    } else {
      return LegacyKeyLock.acquireLegacyWriteLock(metadataManager.getLock(), volume, bucket,
          keys);
    }
  }

  static LockHolder acquireReadLock(OMMetadataManager metadataManager,
                                     KeyArgs keyArgs)
      throws IOException {
    return acquireReadLock(metadataManager, keyArgs.getVolumeName(),
        keyArgs.getBucketName(), keyArgs.getKeyName());
  }

  static LockHolder acquireReadLock(OMMetadataManager metadataManager,
                                     String volume, String bucket,
                                     String... keys)
      throws IOException {
    BucketLayout layout = getBucketLayout(metadataManager, volume, bucket);
    if (layout == OBJECT_STORE) {
      return ObsKeyLock.acquireObsReadLock(metadataManager.getLock(), volume, bucket,
          keys);
    } else if (layout == FILE_SYSTEM_OPTIMIZED) {
      return FsoKeyLock.acquireFsoReadLock(metadataManager.getLock(), volume, bucket,
          keys);
    } else {
      return LegacyKeyLock.acquireLegacyReadLock(metadataManager.getLock(), volume, bucket,
          keys);
    }
  }

}
