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
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.TableFormatConfig;

import javax.annotation.Nullable;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.LEAK_DETECTOR;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.assertClosed;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.formatStackTrace;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.getStackTrace;

/**
 * Managed ColumnFamilyOptions.
 */
public class ManagedColumnFamilyOptions extends ColumnFamilyOptions implements Leakable {

  @Nullable
  private final StackTraceElement[] elements = getStackTrace();

  /**
   * Indicate if this ColumnFamilyOptions is intentionally used across RockDB
   * instances.
   */
  private boolean reused = false;

  public ManagedColumnFamilyOptions() {
    LEAK_DETECTOR.watch(this);
  }

  public ManagedColumnFamilyOptions(ColumnFamilyOptions columnFamilyOptions) {
    super(columnFamilyOptions);
    LEAK_DETECTOR.watch(this);
  }

  @Override
  public synchronized ManagedColumnFamilyOptions setTableFormatConfig(
      TableFormatConfig tableFormatConfig) {
    TableFormatConfig previous = tableFormatConfig();
    if (previous instanceof ManagedBlockBasedTableConfig) {
      if (!((ManagedBlockBasedTableConfig) previous).isClosed()) {
        throw new IllegalStateException("Overriding an unclosed value.");
      }
    } else if (previous != null) {
      throw new UnsupportedOperationException("Overwrite is not supported for "
          + previous.getClass());
    }

    super.setTableFormatConfig(tableFormatConfig);
    return this;
  }

  public synchronized ManagedColumnFamilyOptions closeAndSetTableFormatConfig(
      TableFormatConfig tableFormatConfig) {
    TableFormatConfig previous = tableFormatConfig();
    if (previous instanceof ManagedBlockBasedTableConfig) {
      ((ManagedBlockBasedTableConfig) previous).close();
    }
    setTableFormatConfig(tableFormatConfig);
    return this;
  }

  public void setReused(boolean reused) {
    this.reused = reused;
  }

  public boolean isReused() {
    return reused;
  }

  @Override
  public void check() {
    assertClosed(this, formatStackTrace(elements));
  }

  /**
   * Close ColumnFamilyOptions and its child resources.
   * See org.apache.hadoop.hdds.utils.db.DBProfile.getColumnFamilyOptions
   *
   * @param options
   */
  public static void closeDeeply(ColumnFamilyOptions options) {
    TableFormatConfig tableFormatConfig = options.tableFormatConfig();
    if (tableFormatConfig instanceof ManagedBlockBasedTableConfig) {
      ((ManagedBlockBasedTableConfig) tableFormatConfig).close();
    }
    options.close();
  }
}
