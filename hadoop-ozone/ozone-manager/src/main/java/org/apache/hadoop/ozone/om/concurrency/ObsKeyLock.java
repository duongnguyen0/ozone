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

import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;

import java.util.Arrays;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.KEY_PATH_LOCK;

/**
 * Encapsulate the locking key level ops on OBS bucket, like create, delete
 * or rename keys.
 */
public class ObsKeyLock {
  private ObsKeyLock() {
  }

  /**
   * Acquire a lock for OBS key write scenarios.
   */
  static LockHolder acquireObsWriteLock(OzoneManagerLock lock,
                                        String volume, String bucket,
                                        String... keys) {
    lock.acquireReadLock(BUCKET_LOCK, volume, bucket);
    // Always lock keys in alphabetical order to avoid deadlocks.
    if (keys.length > 1) {
      Arrays.sort(keys);
    }
    for (String key : keys) {
      lock.acquireWriteLock(KEY_PATH_LOCK, volume, bucket, key);
    }
    return () -> {
      lock.releaseReadLock(BUCKET_LOCK, volume, bucket);
      for (String key : keys) {
        lock.releaseWriteLock(KEY_PATH_LOCK, volume, bucket, key);
      }
    };
  }

  static LockHolder acquireObsReadLock(OzoneManagerLock lock,
                                       String volume, String bucket,
                                       String... keys) {
    lock.acquireReadLock(BUCKET_LOCK, volume, bucket);
    // Always lock keys in alphabetical order to avoid deadlocks.
    if (keys.length > 1) {
      Arrays.sort(keys);
    }
    for (String key : keys) {
      lock.acquireReadLock(KEY_PATH_LOCK, volume, bucket, key);
    }
    return () -> {
      lock.releaseReadLock(BUCKET_LOCK, volume, bucket);
      for (String key : keys) {
        lock.acquireReadLock(KEY_PATH_LOCK, volume, bucket, key);
      }
    };
  }
}
