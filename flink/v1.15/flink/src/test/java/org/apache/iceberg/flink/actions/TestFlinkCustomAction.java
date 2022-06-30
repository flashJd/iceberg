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


package org.apache.iceberg.flink.actions;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static org.apache.iceberg.TableProperties.UPSERT_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestFlinkCustomAction {

  private static HiveConf hiveConf = null;
  private static HiveCatalog catalog = null;
  private static TableEnvironment tEnv = null;
  private static final String hiveURL = "thrift://172.16.60.111:9083";
  private static final String hiveWarehouse = "hdfs://172.16.60.111:9000/user/root/hive";
  private static final String DB = "rebel";
  private static final String TBL = "test101";
  private static final HashSet<Integer> identifierFieldIds = new HashSet<>(Arrays.asList(1,2));
  private static final List<Types.NestedField> COLUMNS = new ArrayList<>(Arrays.asList(
          required(1, "c1", Types.IntegerType.get()),
          required(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get())));
  private static final Schema SCHEMA = new Schema(COLUMNS, identifierFieldIds);

  private Boolean isHadoopCatalog = false;
  private  String warehouseLocation = null;
  private  String catalogName = "testhadoop";
  private  Catalog validationCatalog = null;
  private  Map<String, String> catalogConfig = Maps.newHashMap();
  private  Map<String, String> tableConfig = Maps.newHashMap();
  private  Namespace baseNamespace = null;
  private  Namespace icebergNamespace = null;
  private  SupportsNamespaces validationNamespaceCatalog = null;

//  protected List<Row> sql(String query, Object... args) {
//    TableResult tableResult = getTableEnv().executeSql(String.format(query, args));
//    try (CloseableIterator<Row> iter = tableResult.collect()) {
//      return Lists.newArrayList(iter);
//    } catch (Exception e) {
//      throw new RuntimeException("Failed to collect table result", e);
//    }
//  }

  static String toWithClause(Map<String, String> props) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    int propCount = 0;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (propCount > 0) {
        builder.append(",");
      }
      builder.append("'").append(entry.getKey()).append("'").append("=")
              .append("'").append(entry.getValue()).append("'");
      propCount++;
    }
    builder.append(")");
    return builder.toString();
  }

  private static HiveConf initCustomHiveConf() {
    HiveConf conf = new HiveConf(new Configuration(), TestHiveMetastore.class);
    conf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveURL);
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, hiveWarehouse);
    conf.set(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname, "false");
    conf.set(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname, "false");
    conf.set("iceberg.hive.client-pool-size", "2");
    return conf;
  }

  @BeforeClass
  public static void initEnv() {
    EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
    TableEnvironment env = TableEnvironment.create(settings);
    env.getConfig().getConfiguration().set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
    env.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    env.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
    tEnv = env;
    hiveConf = initCustomHiveConf();
    catalog = (HiveCatalog)
            CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  static public StreamExecutionEnvironment getEnv() {
    org.apache.flink.configuration.Configuration confData = new org.apache.flink.configuration.Configuration();
    confData.setString("akka.ask.timeout", "1h");
    confData.setString("akka.watch.heartbeat.interval", "1h");
    confData.setString("akka.watch.heartbeat.pause", "1h");
    confData.setString("heartbeat.timeout", "18000000");
    confData.setString(String.valueOf(CoreOptions.CHECK_LEAKED_CLASSLOADER), "false");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(confData);
    return env;
  }

  @Before
  public void setupCatalogAndDB() throws Exception {
    this.baseNamespace = Namespace.empty();
    isHadoopCatalog = catalogName.startsWith("testhadoop");
//    this.warehouseLocation = new File("data").toURI().toString();
    this.warehouseLocation = "file:/project/iceberg_module/iceberg/spark/v3.2/spark/data";
    this.validationCatalog = isHadoopCatalog ?
            new HadoopCatalog(hiveConf, warehouseLocation) :
            catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    catalogConfig.put("type", "iceberg");
    if (!baseNamespace.isEmpty()) {
      catalogConfig.put(FlinkCatalogFactory.BASE_NAMESPACE, baseNamespace.toString());
    }
    if (isHadoopCatalog) {
      catalogConfig.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop");
      catalogConfig.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    } else {
      catalogConfig.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hive");
      catalogConfig.put(CatalogProperties.URI, hiveURL);
      catalogConfig.put(CatalogProperties.WAREHOUSE_LOCATION, hiveWarehouse);
    }

    this.icebergNamespace = Namespace.of(ArrayUtils.concat(baseNamespace.levels(), new String[] {DB}));

    tEnv.executeSql(String.format("CREATE CATALOG %s WITH %s", catalogName, toWithClause(catalogConfig)));
    tEnv.executeSql(String.format("USE CATALOG %s", catalogName));
    tEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", DB));
    tEnv.executeSql(String.format("USE %s", DB));
    tEnv.executeSql("SHOW CATALOGS").print();
    tEnv.executeSql("SHOW TABLES").print();
    tableConfig.put(DEFAULT_FILE_FORMAT, "parquet");
    tableConfig.put(FORMAT_VERSION, "2");
    tableConfig.put(UPSERT_ENABLED, "true");
    tableConfig.put(METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
    tableConfig.put(METADATA_PREVIOUS_VERSIONS_MAX, "2");
  }

  @Test
  public void testSqlCreateTable() throws Exception {
    tEnv.executeSql(String.format("DROP TABLE IF EXISTS %s", TBL));
    tEnv.executeSql(String.format("CREATE TABLE %s (id int, data varchar) with %s", TBL, toWithClause(tableConfig)));
  }

  @Test
  public void testSqlInsert() {
    tEnv.executeSql(String.format("INSERT INTO %s SELECT 1, 'hello'", TBL));
    tEnv.executeSql(String.format("INSERT INTO %s SELECT 2, 'world'", TBL));
    tEnv.executeSql(String.format("SELECT * FROM %s LIMIT 10", TBL)).print();
  }

  @Test
  public void testCreateTable() {
    PartitionSpec partitionSpec = PartitionSpec.builderFor(SCHEMA).identity("c2").build();
    TableIdentifier tbId = TableIdentifier.of(DB, TBL);
    if (validationCatalog.tableExists(tbId)) {
      validationCatalog.dropTable(tbId);
    }

    Table table = validationCatalog.buildTable(tbId, SCHEMA)
//            .withSortOrder(sortOrder)
            .withPartitionSpec(partitionSpec)
            .withProperties(tableConfig)
            .create();
//    table.updateProperties().set(TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(128*1024)).commit();
    System.out.println(table.location());
  }

  @Test
  public void testStreamWrite() throws Exception {
    TableLoader tableLoader;
    if (isHadoopCatalog) {
      tableLoader = TableLoader.fromHadoopTable(warehouseLocation+ "/" + DB + "/" + TBL, hiveConf);
    } else {
      CatalogLoader hiveCatalogLoader = CatalogLoader.hive(catalog.name(), hiveConf, catalogConfig);
      tableLoader = TableLoader.fromCatalog(hiveCatalogLoader, TableIdentifier.of(DB, TBL));
    }

    StreamExecutionEnvironment env = getEnv();
    env.getCheckpointConfig().setCheckpointInterval(6 * 1000);

    DataStream<RowData> input = env.addSource(new MySrcFunc(2000, 0, 10));
    FlinkSink.forRowData(input)
            .tableLoader(tableLoader)
            .overwrite(false)
            .upsert(true)
            .distributionMode(DistributionMode.HASH)
            .equalityFieldColumns(Lists.newArrayList("c1", "c2"))
            .writeParallelism(1)
            .append();
    env.execute();

  }

  @Test
  public void testCheckpoint() throws Exception {
    TableLoader tableLoader;
    TableLoader tableLoader1;

      tableLoader = TableLoader.fromHadoopTable(warehouseLocation+ "/" + DB + "/" + TBL, hiveConf);
      tableLoader1 = TableLoader.fromHadoopTable(warehouseLocation+ "/" + DB + "/" + TBL, hiveConf);


    StreamExecutionEnvironment env = getEnv();
    env.getCheckpointConfig().setCheckpointInterval(120 * 1000);

    tableLoader1.open();
    Table icebergTable = tableLoader1.loadTable();
    DataStream<RowData> input = env.addSource(new MySrcFunc(1, 0, 1, icebergTable));
    FlinkSink.forRowData(input)
            .tableLoader(tableLoader)
            .overwrite(false)
            .upsert(true)
            .distributionMode(DistributionMode.HASH)
            .equalityFieldColumns(Lists.newArrayList("c1", "c2"))
            .writeParallelism(1)
            .append();

    env.execute();
//    tableLoader.open();
//    Table icebergTable = tableLoader.loadTable();
//    UpdateSchema updateSchema = icebergTable.updateSchema();
//    updateSchema.addColumn("data", Types.StringType.get());
//    updateSchema.commit();
  }
  @Test
  public void testReadTable() throws Exception {
    TableLoader tableLoader;
    if (isHadoopCatalog) {
      tableLoader = TableLoader.fromHadoopTable(warehouseLocation+ "/" + DB + "/" + TBL, hiveConf);
    } else {
      CatalogLoader hiveCatalogLoader = CatalogLoader.hive(catalog.name(), hiveConf, catalogConfig);
      tableLoader = TableLoader.fromCatalog(hiveCatalogLoader, TableIdentifier.of(DB, TBL));
    }

    StreamExecutionEnvironment env = getEnv();
    DataStream<RowData> dataStream = FlinkSource.forRowData()
            .env(env)
            .tableLoader(tableLoader)
            .streaming(false)
            .build();

    CloseableIterator<RowData> dataCloseableIterator = dataStream.executeAndCollect();

    System.out.println("------------------------------------------------------------");
    for (int i=0; i<50 && dataCloseableIterator.hasNext(); i++) {
      RowData d = dataCloseableIterator.next();
      System.out.println(d.getInt(0) + "," + d.getString(1) + "," + d.getString(2));
    }
    System.out.println("------------------------------------------------------------");
  }
}
