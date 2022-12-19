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

/**
 * Represent a lock scope to hide the details of the concrete resource
 * underneath resources.
 * This object is used as a result of acquiring locks and knows how to
 * release the acquired locks.
 *
 * Example usage:
 *
 * <pre> {@code
 * LockHolder holder = acquireLock();
 * try {
 *   // access the resource protected by this lock
 * } finally {
 *   holder.release();
 * }
 * }</pre>
 *
 * Or it can also be used with a try-with-resource statement.
 *
 * <pre> {@code
 *  try (LockHolder ignored = acquireLock()) {
 *    // access the resource protected by this lock
 *  }
 *  }</pre>
 */
@FunctionalInterface
public interface LockHolder extends AutoCloseable {
  void release();

  default void close() {
    release();
  }

  LockHolder EMPTY = () -> {
  };

  static LockHolder aggregate(Iterable<LockHolder> lockHolders) {
    return () -> {
      for (LockHolder holder : lockHolders) {
        holder.release();
      }
    };
  }
}
