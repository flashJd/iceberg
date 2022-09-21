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

package org.apache.iceberg.flink.source;

import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkAvroReader;
import org.apache.iceberg.flink.data.FlinkOrcReader;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class RowDataFileScanTaskReader implements FileScanTaskReader<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(RowDataFileScanTaskReader.class);
  private static final Schema POS_DELETE_SCHEMA = new Schema(
      MetadataColumns.DELETE_FILE_PATH,
      MetadataColumns.DELETE_FILE_POS);
  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;

  public RowDataFileScanTaskReader(
      Schema tableSchema, Schema projectedSchema,
      String nameMapping, boolean caseSensitive) {
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public CloseableIterator<RowData> open(FileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
    Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant = partitionSchema.columns().isEmpty() ? ImmutableMap.of() :
        PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    FlinkDeleteFilter deletes = new FlinkDeleteFilter(task, tableSchema, projectedSchema, inputFilesDecryptor);
    CloseableIterable<RowData> iterable = deletes.filter(
        newIterable(task, deletes.requiredSchema(), idToConstant, inputFilesDecryptor)
    );

    // Project the RowData to remove the extra meta columns.
    if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection = RowDataProjection.create(
          deletes.requiredRowType(), deletes.requiredSchema().asStruct(), projectedSchema.asStruct());
      iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
    }

    return iterable.iterator();
  }

  @Override
  public CloseableIterator<RowData> openDelete(FileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
    Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant = partitionSchema.columns().isEmpty() ? ImmutableMap.of() :
        PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    FlinkDeleteFilter deletes = new FlinkDeleteFilter(task, tableSchema, projectedSchema, inputFilesDecryptor);
    CloseableIterable<RowData> iterable =
        newEqDeleteIterable(task, deletes.requiredSchema(), idToConstant, inputFilesDecryptor);

    // Project the RowData to remove the extra meta columns.
    if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection = RowDataProjection.create(
          deletes.requiredRowType(), deletes.requiredSchema().asStruct(), projectedSchema.asStruct());
      iterable = CloseableIterable.transform(iterable, rowDataProjection::wrapDelete);
    }

    return iterable.iterator();
  }

  private CloseableIterable<RowData> newEqDeleteIterable(
      FileScanTask task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    CloseableIterable<RowData> iter;
    if (task.isDataTask()) {
      throw new UnsupportedOperationException("Cannot read data task.");
    } else {
      switch (task.file().format()) {
        case PARQUET:
          iter = newEqDeleteParquetIterable(task, schema, idToConstant, inputFilesDecryptor);
          break;

        case AVRO:
          iter = newEqDeleteAvroIterable(task, schema, idToConstant, inputFilesDecryptor);
          break;

        case ORC:
          iter = newEqDeleteOrcIterable(task, schema, idToConstant, inputFilesDecryptor);
          break;

        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }
    }

    return iter;
  }

  public Schema newPosDeleteSchema(Schema requestedSchema) {
    ImmutableList<Types.NestedField> newColumns = ImmutableList.<Types.NestedField>builder()
        .addAll(requestedSchema.columns())
        .add(MetadataColumns.ROW_POSITION)
        .add(MetadataColumns.IS_DELETED).build();
    return new Schema(newColumns);
  }

  private CloseableIterable<RowData> newEqDeleteParquetIterable(
      FileScanTask task, Schema schema,
      Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {

    if (task.deletes().stream().noneMatch(file -> file.content().equals(FileContent.EQUALITY_DELETES))) {
      return getDeletedRows(task, schema, inputFilesDecryptor);
    }

    List<InputFile> inputFiles = inputFilesDecryptor.getEqDeleteInputFile(task);
    LOG.info("Schema: {}, List<InputFile>: {}.", schema, inputFiles);

    Iterable<CloseableIterable<RowData>> iterable = Iterables.transform(inputFiles, inputFile -> {
      Parquet.ReadBuilder builder = Parquet.read(inputFile)
          .reuseContainers()
          .project(schema)
          .createReaderFunc(fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema))
          .caseSensitive(caseSensitive)
          .reuseContainers();

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }
      return builder.build();
    });

    return CloseableIterable.concat(iterable);
  }

  private CloseableIterable<RowData> getDeletedRows(FileScanTask task, Schema schema, InputFilesDecryptor decryptor) {
    Schema requiredSchema = newPosDeleteSchema(projectedSchema);
    RowDataWrapper wrap = new RowDataWrapper(FlinkSchemaUtil.convert(requiredSchema), requiredSchema.asStruct());
    Accessor<StructLike> ass = requiredSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());

    Iterable<CloseableIterable<RowData>> transform =
        Iterables.transform(decryptor.getPosDeleteInputFile(task), inputFile -> {
          // read pos delete files
          CloseableIterable<Record> deleteFilesRecords = Parquet.read(inputFile)
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(POS_DELETE_SCHEMA, fileSchema))
              .reuseContainers().project(POS_DELETE_SCHEMA).build();

          CloseableIterable<CloseableIterable<RowData>> deletedRows =
              CloseableIterable.transform(
                  deleteFilesRecords,
                  (Function<Record, CloseableIterable<RowData>>) record -> {
                    //  Read the data file pointed to by the pos delete file_path, and then filter the data to be
                    //  deleted from the pos file after reading the data file
                    CloseableIterable<RowData> rows = Parquet.read(HadoopInputFile.fromLocation(
                            record.getField("file_path").toString(), new Configuration()))
                        .createReaderFunc(fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema))
                        .reuseContainers().project(schema).build();
                    return CloseableIterable.filter(
                        rows, rowData -> (long) ass.get(wrap.wrap(rowData)) == (long) record.getField("pos"));
                  });

          return CloseableIterable.concat(deletedRows);
        });

    return CloseableIterable.concat(transform);
  }

  private CloseableIterable<RowData> newEqDeleteAvroIterable(
      FileScanTask task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    if (task.deletes().stream().noneMatch(file -> file.content().equals(FileContent.EQUALITY_DELETES))) {
      return CloseableIterable.empty();
    }

    List<InputFile> inputFiles = inputFilesDecryptor.getEqDeleteInputFile(task);
    Iterable<CloseableIterable<RowData>> iterable = Iterables.transform(inputFiles, inputFile -> {
      Avro.ReadBuilder builder = Avro.read(inputFile)
          .reuseContainers()
          .project(schema)
          //                .split(task.start(), task.length())
          .createReaderFunc(readSchema -> new FlinkAvroReader(schema, readSchema));

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }
      return builder.build();
    });

    return CloseableIterable.concat(iterable);
  }

  private CloseableIterable<RowData> newEqDeleteOrcIterable(
      FileScanTask task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    if (task.deletes().stream().noneMatch(file -> file.content().equals(FileContent.EQUALITY_DELETES))) {
      return CloseableIterable.empty();
    }

    Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(
        schema,
        Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    List<InputFile> inputFiles = inputFilesDecryptor.getEqDeleteInputFile(task);
    Iterable<CloseableIterable<RowData>> iterable = Iterables.transform(inputFiles, inputFile -> {
      ORC.ReadBuilder builder = ORC.read(inputFile)
          .project(readSchemaWithoutConstantAndMetadataFields)
          //                .split(task.start(), task.length())
          .createReaderFunc(readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema))
          //              .filter(task.residual())
          .caseSensitive(caseSensitive);

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }
      return builder.build();
    });

    return CloseableIterable.concat(iterable);
  }

  private CloseableIterable<RowData> newIterable(
      FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
    CloseableIterable<RowData> iter;
    if (task.isDataTask()) {
      throw new UnsupportedOperationException("Cannot read data task.");
    } else {
      switch (task.file().format()) {
        case PARQUET:
          iter = newParquetIterable(task, schema, idToConstant, inputFilesDecryptor);
          break;

        case AVRO:
          iter = newAvroIterable(task, schema, idToConstant, inputFilesDecryptor);
          break;

        case ORC:
          iter = newOrcIterable(task, schema, idToConstant, inputFilesDecryptor);
          break;

        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }
    }

    return iter;
  }

  private CloseableIterable<RowData> newAvroIterable(
      FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
    Avro.ReadBuilder builder = Avro.read(inputFilesDecryptor.getInputFile(task))
        .reuseContainers()
        .project(schema)
        .split(task.start(), task.length())
        .createReaderFunc(readSchema -> new FlinkAvroReader(schema, readSchema, idToConstant));

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newParquetIterable(
      FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
    Parquet.ReadBuilder builder = Parquet.read(inputFilesDecryptor.getInputFile(task))
        .reuseContainers()
        .split(task.start(), task.length())
        .project(schema)
        .createReaderFunc(fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
        .filter(task.residual())
        .caseSensitive(caseSensitive)
        .reuseContainers();

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newOrcIterable(
      FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
    Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(
        schema,
        Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder = ORC.read(inputFilesDecryptor.getInputFile(task))
        .project(readSchemaWithoutConstantAndMetadataFields)
        .split(task.start(), task.length())
        .createReaderFunc(readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
        .filter(task.residual())
        .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private static class FlinkDeleteFilter extends DeleteFilter<RowData> {
    private final RowType requiredRowType;
    private final RowDataWrapper asStructLike;
    private final InputFilesDecryptor inputFilesDecryptor;

    FlinkDeleteFilter(
        FileScanTask task, Schema tableSchema, Schema requestedSchema,
        InputFilesDecryptor inputFilesDecryptor) {
      super(task, tableSchema, requestedSchema);
      this.requiredRowType = FlinkSchemaUtil.convert(requiredSchema());
      this.asStructLike = new RowDataWrapper(requiredRowType, requiredSchema().asStruct());
      this.inputFilesDecryptor = inputFilesDecryptor;
    }

    public RowType requiredRowType() {
      return requiredRowType;
    }

    @Override
    protected StructLike asStructLike(RowData row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return inputFilesDecryptor.getInputFile(location);
    }
  }
}
