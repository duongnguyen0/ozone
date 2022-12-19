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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Encapsulate the locking logic for bucket level ops, like create and modify
 * buckets.
 */
public final class BucketLock {
  private BucketLock() {
  }

  /**
   * Acquire lock for bucket write scenarios.
   * @return a LockHolder that knows how to release the acquired lock.
   */
  static LockHolder acquireWriteLock(OzoneManagerLock ozoneManagerLock,
                                   String volume, String bucket) {
    ozoneManagerLock.acquireReadLock(VOLUME_LOCK, volume);
    ozoneManagerLock.acquireWriteLock(BUCKET_LOCK, volume, bucket);
    return () -> {
      ozoneManagerLock.releaseReadLock(VOLUME_LOCK, volume);
      ozoneManagerLock.releaseWriteLock(BUCKET_LOCK, volume, bucket);
    };
  }

  /**
   * Acquire lock for bucket read scenarios.
   * @return a LockHolder that knows how to release the acquired lock.
   */
  static LockHolder acquireReadLock(OzoneManagerLock ozoneManagerLock,
                                     String volume, String bucket) {
    ozoneManagerLock.acquireReadLock(BUCKET_LOCK, volume, bucket);
    return () -> ozoneManagerLock.releaseReadLock(BUCKET_LOCK, volume, bucket);
  }

}
