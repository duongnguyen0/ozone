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

import static com.google.common.base.Verify.verify;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Encapsulate the locking logic for volume level ops, like create and modify
 * volumes.
 */
public class VolumeLock {

  /**
   * Acquire lock for volume write scenarios.
   * @return a LockHolder that knows how to release the acquired lock.
   */
  static LockHolder acquireWriteLock(OzoneManagerLock ozoneManagerLock,
                                   String volume) {
    boolean acquired = ozoneManagerLock.acquireWriteLock(VOLUME_LOCK, volume);
    verify(acquired, "Volume write lock can not be acquired");
    return () -> ozoneManagerLock.releaseWriteLock(VOLUME_LOCK, volume);
  }

  /**
   * Acquire lock for volume read scenarios.
   * @return a LockHolder that knows how to release the acquired lock.
   */
  static LockHolder acquireReadLock(OzoneManagerLock ozoneManagerLock,
                                     String volume) {
    boolean acquired = ozoneManagerLock.acquireReadLock(VOLUME_LOCK, volume);
    verify(acquired, "Volume read lock can not be acquired");
    return () -> ozoneManagerLock.releaseReadLock(VOLUME_LOCK, volume);
  }

}
