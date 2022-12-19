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

/**
 * Encapsulate the locking key level ops on Legacy bucket, like create, delete
 * or rename keys.
 */
public class LegacyKeyLock {
  /**
   * Acquire a lock for Legacy key write scenarios.
   */
  static LockHolder acquireLegacyWriteLock(OzoneManagerLock lock,
                                           String volume, String bucket,
                                           String... keys) {
    // For legacy, we don't optimize to use fine-grained lock, still
    // serialize requests for same bucket.
    lock.acquireWriteLock(BUCKET_LOCK, volume, bucket);
    return () -> lock.releaseWriteLock(BUCKET_LOCK, volume, bucket);
  }

  /**
   * Acquire a lock for Legacy key write scenarios.
   */
  static LockHolder acquireLegacyReadLock(OzoneManagerLock lock,
                                           String volume, String bucket,
                                           String... keys) {
    // For legacy, we don't optimize to use fine-grained lock, still
    // serialize requests for same bucket.
    lock.acquireReadLock(BUCKET_LOCK, volume, bucket);
    return () -> lock.releaseReadLock(BUCKET_LOCK, volume, bucket);
  }
}
