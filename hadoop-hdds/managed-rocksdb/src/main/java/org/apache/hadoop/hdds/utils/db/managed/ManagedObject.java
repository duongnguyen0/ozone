/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.hadoop.hdds.resource.Leakable;
import org.rocksdb.RocksObject;

import javax.annotation.Nullable;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.LEAK_DETECTOR;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.formatStackTrace;

/**
 * General template for a managed RocksObject.
 * @param <T>
 */
class ManagedObject<T extends RocksObject> implements AutoCloseable, Leakable {
  private final T original;

  @Nullable
  private final StackTraceElement[] elements;

  ManagedObject(T original) {
    this.original = original;
    this.elements = ManagedRocksObjectUtils.getStackTrace();
    LEAK_DETECTOR.watch(this);
  }

  public T get() {
    return original;
  }

  @Override
  public void close() {
    original.close();
  }

  @Override
  public void check() {
    ManagedRocksObjectUtils.assertClosed(this);
  }

  public String getStackTrace() {
    return formatStackTrace(elements);
  }

}
