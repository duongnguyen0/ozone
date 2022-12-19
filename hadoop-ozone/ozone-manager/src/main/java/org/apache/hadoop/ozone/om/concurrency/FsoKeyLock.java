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
 * Encapsulate the locking key level ops on FSO bucket, like create, delete
 * or rename keys.
 */
public final class FsoKeyLock {
  private FsoKeyLock() {
  }

  /**
   * Acquire a lock for FSO key write scenarios.
   */
  static LockHolder acquireFsoWriteLock(OzoneManagerLock lock,
                                        String volume, String bucket,
                                        String... ignoredKeys) {
    // TODO: optimize to use fine-grained lock for FSO.
    lock.acquireWriteLock(BUCKET_LOCK, volume, bucket);
    return () -> lock.releaseWriteLock(BUCKET_LOCK, volume, bucket);
  }

  /**
   * Acquire a lock for FSO key write scenarios.
   */
  static LockHolder acquireFsoReadLock(OzoneManagerLock lock,
                                       String volume, String bucket,
                                       String... ignoredKeys) {
    // TODO: optimize to use fine-grained lock for FSO.
    lock.acquireReadLock(BUCKET_LOCK, volume, bucket);
    return () -> lock.acquireReadLock(BUCKET_LOCK, volume, bucket);
  }
}
