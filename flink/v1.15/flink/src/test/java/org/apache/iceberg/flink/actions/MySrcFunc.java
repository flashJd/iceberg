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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Types;

public class MySrcFunc implements SourceFunction<RowData> {

  private static final String[] partitionValues = {
          "zhejiang" ,"anhui", "jiangsu", "shanghai", "hubei",
          "shandong", "jiangxi", "guangdong"};

  int count;
  int mode;   /* 0 insert, 1 delete  2update */
  int deleteCount;
  Random ran ;
  Table tb;

  public MySrcFunc(int count, int mode, int deleteCount) {
    this.count = count;
    this.mode = mode;
    this.deleteCount = deleteCount;
    this.ran = new Random(System.currentTimeMillis());
  }

  public MySrcFunc(int count, int mode, int deleteCount, Table icebergTable) {
    this.count = count;
    this.mode = mode;
    this.deleteCount = deleteCount;
    this.ran = new Random(System.currentTimeMillis());
    this.tb = icebergTable;
  }

  @Override
  public void run(SourceContext<RowData> sourceContext) throws Exception {
    switch (mode) {
      case 0:
        for (int i = 0; i < deleteCount; i++) {
          sourceContext.collect(insertRowForDelete(i));
        }

//        UpdateSchema updateSchema = tb.updateSchema();
//        updateSchema.addColumn("data", Types.StringType.get());
//        updateSchema.commit();
//        TimeUnit.SECONDS.sleep(2);
//        for (int i = 0; i < deleteCount; i++) {
//          sourceContext.collect(deleteRow1(i));
//        }

//        for (int i = deleteCount; i < count + deleteCount; i++) {
//          sourceContext.collect(insertRow(i));
//        }
        break;
      case 1:
        for (int i = 0; i < deleteCount; i++) {
            sourceContext.collect(deleteRow(i));
        }
        break;
//      case 2:
//        for (int i = 0; i < count; i++) {
//          sourceContext.collect(deleteRow(i));
//        }
//        break;
      default:
        System.out.println("invalid input mode value is " + mode);
        break;
    }
  }

  private GenericRowData insertRowForDelete(int id) {
    GenericRowData rowData = new GenericRowData(RowKind.INSERT, 3);
    rowData.setField(0, id);
    rowData.setField(1, StringData.fromString(partitionValues[1]));
    rowData.setField(2, StringData.fromString("for delete"));
    return rowData;
  }

  private GenericRowData deleteRow1(int id) {
    GenericRowData rowData = new GenericRowData(RowKind.DELETE, 4);
    rowData.setField(0, id);
    rowData.setField(1, StringData.fromString(partitionValues[1]));
    rowData.setField(2, StringData.fromString("for delete"));
    rowData.setField(3, StringData.fromString("ssss"));
    return rowData;
  }

  private GenericRowData insertRow(int id) {
    GenericRowData rowData = new GenericRowData(RowKind.INSERT, 3);

    String tmpKey = RandomStringUtils.randomAlphanumeric(10);

    rowData.setField(0, id);
    rowData.setField(1, StringData.fromString(partitionValues[ran.nextInt(partitionValues.length)]));
    rowData.setField(2, StringData.fromString(tmpKey));

    return rowData;
  }

  private GenericRowData deleteRow(int id) {
    GenericRowData rowData = new GenericRowData(RowKind.DELETE, 3);
    rowData.setField(0, id);
    rowData.setField(1, StringData.fromString(partitionValues[0]));
    rowData.setField(2, StringData.fromString("for delete"));
    return rowData;
  }

  @Override
  public void cancel() {

  }

}

