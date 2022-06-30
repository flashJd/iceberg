/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.actions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;


public class TestFlinkOpt {
//  static String tblLocal = warehouse + "/" + db + "/" + table;
  static String db;
  static String table;
  static String tblLocal;
  static String hadoopCatalog;

  @Before
  public void enn() {
//    String warehouse = "/Users/wuwenchi/work/iceberg/flink/v1.13/flink/src/test/java/org/apache/iceberg/flink/actions/warehouse";
    String warehouse = "/projects/dlink/dlink-iceberg-0.13.1/spark/warehouse";
    db = "db";
    table = "tb1";
    String hdfs = "hdfs://10.0.30.14:9000/user/hive/warehouse";
    boolean useLocal = true;
    if (useLocal) {
      tblLocal = warehouse + "/" + db + "/" + table;
      hadoopCatalog = warehouse;
    } else {
      tblLocal = hdfs + "/" + db + "/" + table;
      hadoopCatalog = hdfs;
    }
  }

  @Test
  public void testFlinkOpt() throws Exception {
//    createTable();
    writeTableStream(10000, 0, 5);
//    writeTableBatch(10);
//    readTable();
//    rewriteTable(tableLocal);
//    optWithSql();
  }

  public void writeTableStream(int number, int mode, int delPos) throws Exception {
    TableLoader sourceTable = TableLoader.fromHadoopTable(tblLocal, new Configuration());
    StreamExecutionEnvironment env = getEnv();
    env.getCheckpointConfig().setCheckpointInterval(6 * 1000);

    DataStream<RowData> input = env.addSource(new MySrcFunc(number, mode, delPos));
    FlinkSink.forRowData(input)
            .tableLoader(sourceTable)
            .overwrite(false)
            .distributionMode(DistributionMode.HASH)
            .equalityFieldColumns(Lists.newArrayList("c1", "c2"))
            .writeParallelism(1)
            .append();
    env.execute();
  }

  public void writeTableBatch(int number) throws Exception {
    TableLoader sourceTable = TableLoader.fromHadoopTable(tblLocal, new Configuration());
    StreamExecutionEnvironment env = getEnv();

    env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    Random rn = new Random();
    int[] arr = new int[number];
    for (int i = 0; i < arr.length; i++) {
//      arr[i] = rn.nextInt(50);
      arr[i] = (int)Math.random() * 10;
    }

//    DataStream<Integer> dsi = env.fromElements(1,2,3,3,2,1);
    DataStream<Long> dsi = env.fromSequence(0, 12);

    SingleOutputStreamOperator<RowData> rowDataStream = dsi.map(new MapFunction<Long, RowData>() {
      @Override
      public RowData map(Long integer) throws Exception {
        GenericRowData rowData =  new GenericRowData(3);
        rowData.setField(0, integer);
        rowData.setField(1, StringData.fromString("aaa" + integer));
        rowData.setField(2, StringData.fromString("bbb" + integer));

        return rowData;
      }
    });

    System.out.println("data has finished");
//    Thread.sleep(10000);


    FlinkSink.forRowData(rowDataStream)
            .tableLoader(sourceTable)
            .overwrite(false)
            .distributionMode(DistributionMode.HASH)
//            .equalityFieldColumns(Lists.newArrayList("c2"))
            .append();
    env.execute();

//    Thread.sleep(999999999);
  }

  @Test
  public void createTable() {
    Configuration conf = new Configuration();
    Catalog catalog = new HadoopCatalog(conf, hadoopCatalog);

    boolean isTableExist = catalog.tableExists(TableIdentifier.of(db, table));

    if (isTableExist == true) {
      System.out.println("Table exist  = " + isTableExist);
      return;
    }

    //创建表
    List<Types.NestedField> columns = new ArrayList<>();
    columns.add(Types.NestedField.required(1, "c1", Types.StringType.get()));
    columns.add(Types.NestedField.required(2, "c2", Types.StringType.get()));
    columns.add(Types.NestedField.required(3, "c3", Types.StringType.get()));

    HashSet<Integer> identifierFieldIds = new HashSet<>();
    identifierFieldIds.add(1);
    identifierFieldIds.add(2);
    Schema schema = new Schema(columns, identifierFieldIds);

    //指定分区键列
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).
            identity("c1").
            build();

    Map<String, String> tableProperties = Maps.newHashMap();
//   tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true"); // engine.hive.enabled=true
//   tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
    tableProperties.put(TableProperties.FORMAT_VERSION, "2");//v2 版本才支持delete file
    tableProperties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(1024*1024*1024*6));
//   tableProperties.put(TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(128*1024));
    tableProperties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(1024*1024*1024*5));
//    tableProperties.put(TableProperties.RECORDS_EACH_PARTITION, String.valueOf(50000));
//    tableProperties.put(TableProperties.FALSE_POSITIVE_PROBABILITY, String.valueOf(0.001));
//    tableProperties.put(TableProperties.WRITE_DISTRIBUTION_MODE, "hash");
    String dbName = db;
    String tableName = table;

    TableIdentifier tblId = TableIdentifier.of(dbName, tableName);
    if (catalog.tableExists(tblId)) {
      catalog.dropTable(tblId);
    }

    Table table = catalog.buildTable(tblId, schema)
//            .withSortOrder(sortOrder)
            .withPartitionSpec(partitionSpec)
            .withProperties(tableProperties)
            .create();
    table.updateProperties().set(TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(128*1024)).commit();
    System.out.println(table.location());
  }

  @Test
  public void readTable() throws Exception {
    TableLoader sourceTable = TableLoader.fromHadoopTable(tblLocal, new Configuration());
    StreamExecutionEnvironment env = getEnv();

    DataStream<RowData> dataStream = FlinkSource.forRowData()
            .env(env)
            .tableLoader(sourceTable)
            .streaming(false)
            .build();
    // TODO 如果数据量特别大的话，collect 会出现爆内存吗？
    CloseableIterator<RowData> dataCloseableIterator = dataStream.executeAndCollect();
    int i=0;

    System.out.println("------------------------------------------------------------");
    while (i<50 && dataCloseableIterator.hasNext()) {
      RowData d = dataCloseableIterator.next();
      System.out.println(d.getInt(0) + "," + d.getString(1));
      i++;
    }
    System.out.println("------------------------------------------------------------");
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

  @Test
  public void rewriteTable() {
    TableLoader sourceTable = TableLoader.fromHadoopTable(tblLocal, new Configuration());
    sourceTable.open();
    Table table = sourceTable.loadTable();
    table.refresh();
    System.out.println(table.currentSnapshot());
    StreamExecutionEnvironment env = getEnv();
    env.setParallelism(2);
    RewriteDataFilesActionResult rewriteDataFilesActionResult = Actions.forTable(env, table)
            .rewriteDataFiles()
            .targetSizeInBytes(40 * 1024)
            .splitOpenFileCost(1)
            .execute();
    System.out.println(rewriteDataFilesActionResult.deletedDataFiles().size());
    for (DataFile dataFile : rewriteDataFilesActionResult.deletedDataFiles()) {
      System.out.println(dataFile.path().toString());
    }
    System.out.println(rewriteDataFilesActionResult.addedDataFiles().size());
    for (DataFile dataFile : rewriteDataFilesActionResult.addedDataFiles()) {
      System.out.println(dataFile.path().toString());
    }
  }

  @Test
  public void optWithSql() {
            EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
//            .inStreamingMode()
            .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    org.apache.flink.configuration.Configuration configuration = tableEnv
            .getConfig()
            .getConfiguration();
    configuration.setString("table.dynamic-table-options.enabled", "true");

    TableResult tableResult = tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
            "  'type'='iceberg',\n" +
            "  'catalog-type'='hadoop',\n" +
            "  'warehouse'='" + hadoopCatalog + "',\n" +
            "  'property-version'='1'\n" +
            ")");
    tableResult.print();
    tableEnv.executeSql("USE CATALOG hadoop_catalog");
    tableEnv.executeSql("SHOW databases").print();
    tableEnv.useDatabase(db);
    tableEnv.executeSql("SHOW TABLES").print();
//    tableEnv.executeSql("create table dddd(id int) \n")
//            .print();
//    tableEnv.executeSql("insert into dddd values(1),(2) \n")
//            .print();
    tableEnv.executeSql("SELECT * FROM tb limit 10 \n")
            .print();
  }
}
