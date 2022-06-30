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

package org.apache.iceberg.spark.actions;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static org.apache.iceberg.TableProperties.UPSERT_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSparkCustomAction {

  protected static HiveConf hiveConf = null;
  protected static SparkSession spark = null;
  protected static SparkSessionCatalog cat = null;
  protected static SparkCatalog dog = null;
  protected static HiveCatalog catalog = null;
  protected static Boolean isHadoopCatalog = true;
  protected static Schema schema = null;
  protected static final String DB = "rebel";
  protected static final String TBL = "test101";
  private static final String hiveURL = "thrift://172.16.60.111:9083";
  private static final String hiveWarehouse = "hdfs://172.16.60.111:9000/user/root/hive";

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
    hiveConf = initCustomHiveConf();

    spark = SparkSession.builder()
            .master("local[2]")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .enableHiveSupport()
            .getOrCreate();

    catalog = (HiveCatalog)
            CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  private String tableLocation = null;
  private Identifier id = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = new File("data");
    this.tableLocation = tableDir.toURI().toString();
    String[] database = {DB};
    this.id = Identifier.of(database, TBL);
  }

  private void hadoopCatalogConf() {
    spark.conf().set("spark.sql.catalog.my_hadoop", "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog.my_hadoop.type", "hadoop");
    spark.conf().set("spark.sql.catalog.my_hadoop.warehouse", tableLocation);
    dog = (SparkCatalog) spark.sessionState().catalogManager().catalog("my_hadoop");
  }

  private void hiveCatalogConf() {
    spark.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    cat = (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
  }

  @Test
  public void testCreateTable() throws Exception {
    Map<String, String> options = Maps.newHashMap();
    options.put(METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
    options.put(METADATA_PREVIOUS_VERSIONS_MAX, "1");
    options.put(FORMAT_VERSION, "2");
    options.put(UPSERT_ENABLED, "true");
    Transform[] transforms = {};
    List<Types.NestedField> columns = Arrays.asList(
            required(1, "c1", Types.IntegerType.get()),
            required(2, "c2", Types.StringType.get()),
            optional(3, "c3", Types.StringType.get()));
    HashSet<Integer> identifierFieldIds = new HashSet<>(Arrays.asList(1,2));
    schema = new Schema(columns, identifierFieldIds);

    if (isHadoopCatalog) {
      hadoopCatalogConf();
      dog.dropTable(id);
      dog.createTable(id, SparkSchemaUtil.convert(schema), transforms, options);
    } else {
      hiveCatalogConf();
      cat.dropTable(id);
      cat.createTable(id, SparkSchemaUtil.convert(schema), transforms, options);
    }
  }

  @Test
  public void testInsert() throws Exception {
    if (isHadoopCatalog) {
      hadoopCatalogConf();
      SparkTable table = (SparkTable) dog.loadTable(id);
      spark.sql("INSERT INTO my_hadoop.rebel.test101 VALUES (1,1,1)");
      spark.sql("SELECT * FROM my_hadoop.rebel.test101").collectAsList().forEach(System.out::println);
    } else {
      hiveCatalogConf();
      SparkTable table = (SparkTable) cat.loadTable(id);
      spark.sql("INSERT INTO spark_catalog.rebel.test101 VALUES (1,1,1)");
      spark.sql("SELECT * FROM spark_catalog.rebel.test101").collectAsList().forEach(System.out::println);
    }
  }

  @Test
  public void testRewriteDataFile() throws Exception {
    SparkTable table;
    if (isHadoopCatalog) {
      hadoopCatalogConf();
      table = dog.loadTable(id);
    } else {
      hiveCatalogConf();
      table = (SparkTable)cat.loadTable(id);
    }

    RewriteDataFiles.Result results = SparkActions.get().rewriteDataFiles(table.table())
            .option(BinPackStrategy.MIN_INPUT_FILES, "2")
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(1024*1024))
//            .option(BinPackStrategy.MAX_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE))
//            .option(BinPackStrategy.DELETE_FILE_THRESHOLD, "2").execute();
            .execute();
    System.out.println("#######addedDataFilesCount:" + results.addedDataFilesCount());
    System.out.println("#######rewrittenDataFilesCount:" + results.rewrittenDataFilesCount());
  }

  @Test
  public void testRewriteManifest() throws Exception {
    SparkTable table;
    if (isHadoopCatalog) {
      hadoopCatalogConf();
      table = dog.loadTable(id);
    } else {
      hiveCatalogConf();
      table = (SparkTable)cat.loadTable(id);
    }

    RewriteManifests.Result results = SparkActions.get().rewriteManifests(table.table()).execute();
    System.out.println("#######addedManifestsCount:");
    results.addedManifests().forEach(System.out::println);
    System.out.println("#######deletedManifestsCount:");
    results.rewrittenManifests().forEach(System.out::println);
  }

  @Test
  public void testExpireSnapShots() throws Exception {
    SparkTable table;
    if (isHadoopCatalog) {
      hadoopCatalogConf();
      table = dog.loadTable(id);
    } else {
      hiveCatalogConf();
      table = (SparkTable)cat.loadTable(id);
    }

    ExpireSnapshots.Result results = SparkActions.get().expireSnapshots(table.table())
//            .expireSnapshotId(table.table().currentSnapshot().snapshotId())
            .expireOlderThan(System.currentTimeMillis() + 1000)
            .execute();
    System.out.println("#######deletedDataFilesCount:" + results.deletedDataFilesCount());
    System.out.println("#######deletedManifestsCount:" + results.deletedManifestsCount());
    System.out.println("#######deletedManifestListsCount:" + results.deletedManifestListsCount());
  }

  @Test
  public void testRollbackSnapShot() throws Exception {
    SparkTable table;
    if (isHadoopCatalog) {
      hadoopCatalogConf();
      table = dog.loadTable(id);
    } else {
      hiveCatalogConf();
      table = (SparkTable)cat.loadTable(id);
    }

    Table tb = table.table();
    System.out.println("#######before: current snapshotId:" + tb.currentSnapshot().snapshotId());
    long target_snapshot = tb.currentSnapshot().parentId();
    tb.rollback().toSnapshotId(target_snapshot).commit();
    System.out.println("#######after: current snapshotId:" + tb.currentSnapshot().snapshotId());
//    System.out.println("#######deletedDataFilesCount:" + results.deletedDataFilesCount());
//    System.out.println("#######deletedManifestsCount:" + results.deletedManifestsCount());
//    System.out.println("#######deletedManifestListsCount:" + results.deletedManifestListsCount());
  }


  @Test
  public void testDeleteOrphanFiles() throws Exception {
    SparkTable table;
    if (isHadoopCatalog) {
      hadoopCatalogConf();
      table = dog.loadTable(id);
      String location = table.table().location().replaceFirst("file:", "");
      new File(location + "/data/trashfile").createNewFile();
    } else {
      hiveCatalogConf();
      table = (SparkTable)cat.loadTable(id);
      FileSystem hdfs = FileSystem.get(new URI("hdfs://172.16.60.111:9000"), hiveConf);
      String location = table.table().location();
      Path file = new Path(location + "/data/trashfile");
      if (hdfs.exists(file)) {
        hdfs.delete( file, true );
      }
      hdfs.create(file);
//      Dataset ds1 = spark.sql("select * from rebel.test101").repartition(2);
//      Dataset ds12 = spark.sql("select * from rebel.test101").repartition(2);
//      ds1.unionAll(ds12).coalesce(2).collectAsList();

    }

    DeleteOrphanFiles.Result results = SparkActions.get().deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000).execute();
    results.orphanFileLocations().forEach(System.out::println);
  }

  @After
  public void resetSparkSessionCatalog() {
    spark.conf().unset("spark.sql.catalog.spark_catalog");
    spark.conf().unset("spark.sql.catalog.spark_catalog.type");
    spark.conf().unset("spark.sql.catalog.spark_catalog.warehouse");
  }

}
