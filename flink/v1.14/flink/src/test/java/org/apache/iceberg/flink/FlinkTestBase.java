/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink;

import java.time.Duration;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public abstract class FlinkTestBase extends TestBaseUtils {

  @ClassRule
  public static MiniClusterWithClientResource miniClusterResource =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static HiveCatalog catalog = null;

  private volatile TableEnvironment tEnv = null;

  private static HiveConf initCustomHiveConf() {
    HiveConf conf = new HiveConf(new Configuration(), TestHiveMetastore.class);
    conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://172.16.60.111:9083");
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "s3a://iceberg/");
    conf.set(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname, "false");
    conf.set(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname, "false");
    conf.set("iceberg.hive.client-pool-size", "2");
    return conf;
  }

  @BeforeClass
  public static void startMetastore() {
//    FlinkTestBase.metastore = new TestHiveMetastore();
//    metastore.start();
//    FlinkTestBase.hiveConf = metastore.hiveConf();
    FlinkTestBase.hiveConf = initCustomHiveConf();
    FlinkTestBase.catalog = (HiveCatalog)
        CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }

  @AfterClass
  public static void stopMetastore() throws Exception {
//    metastore.stop();
    FlinkTestBase.catalog = null;
  }

  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings settings = EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inStreamingMode()
              .build();

          TableEnvironment env = TableEnvironment.create(settings);
          env.getConfig().getConfiguration().set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
          env.getConfig().getConfiguration().set(org.apache.flink.streaming.api.environment
                  .ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(3));
          env.getConfig().getConfiguration().set(org.apache.flink.table.api.config.TableConfigOptions
                  .TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
          env.getConfig().getConfiguration().setString("akka.ask.timeout", "1h");
          env.getConfig().getConfiguration().setString("akka.watch.heartbeat.interval", "1h");
          env.getConfig().getConfiguration().setString("akka.watch.heartbeat.pause", "1h");
          env.getConfig().getConfiguration().setString("heartbeat.timeout", "18000000");
          tEnv = env;
        }
      }
    }
    return tEnv;
  }

  protected static TableResult exec(TableEnvironment env, String query, Object... args) {
    return env.executeSql(String.format(query, args));
  }

  protected TableResult exec(String query, Object... args) {
    return exec(getTableEnv(), query, args);
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = exec(query, args);
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }
}
