/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.sfmc.sink;

import com.exacttarget.fuelsdk.ETDataExtensionColumn;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordDataExtensionRowConverterTest {
  @Test
  public void testGetType() {
    Schema.Type type = Schema.Type.STRING;
    String fieldName = "Name";
    Object val1 = "name";
    String strVal = (String) val1;

    Schema.Type type2 = Schema.Type.LONG;
    String fieldName2 = "price";
    Object val2 = "100";

    Schema.Type type3 = Schema.Type.ENUM;
    String fieldName3 = "price";
    Object val3 = Days.Monday;


    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    ETDataExtensionColumn column = new ETDataExtensionColumn();
    column.setId("121");
    column.setDescription("newCustomer");
    column.setLength(22);
    Date d = new Date(2022, 02, 9);
    column.setCreatedDate(d);
    column.setDefaultValue("default");
    column.setIsPrimaryKey(true);
    column.setIsRequired(true);
    column.setKey("key");
    column.setModifiedDate(d);
    column.setName("name");
    column.setModified("date", false);
    column.setPrecision(0);
    column.setScale(100);
    columnList.add(column);
    Map<String, ETDataExtensionColumn> columns = new HashMap<>();
    columns.put("tableName", column);
    String externalKey = "externalKey";
    DataExtensionInfo dataExtensionInfo = new DataExtensionInfo(externalKey, columnList);
    RecordDataExtensionRowConverter recorddataextensionrowconverter =
      new RecordDataExtensionRowConverter(dataExtensionInfo, true);
    Assert.assertEquals(recorddataextensionrowconverter.getType(type, fieldName, val1), strVal);
    Assert.assertEquals(recorddataextensionrowconverter.getType(type2, fieldName2, val2), String.valueOf(val2));
    Assert.assertEquals(recorddataextensionrowconverter.getType(type3, fieldName3, val3), ((Enum) val3).name());
  }

  @Test
  public void testTransform() throws ETSdkException {
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    ETDataExtensionColumn column1 = new ETDataExtensionColumn();
    column1.setName("storeid");
    column1.setName("emailid");
    column1.setName("timestamp");
    column1.setName("price");
    ETDataExtensionColumn column2 = new ETDataExtensionColumn();
    column2.setName("date");
    columnList.add(column1);
    columnList.add(column2);
    Schema inputSchema = Schema.recordOf("record",
      Schema.Field.of("storeid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.nullableOf(Schema.decimalOf(4, 2))),
      Schema.Field.of("timestamp",
        Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)));
    DataExtensionInfo dataExtensionInfo = new DataExtensionInfo("DE", columnList);
    RecordDataExtensionRowConverter recordDataExtensionRowConverter =
      new RecordDataExtensionRowConverter(dataExtensionInfo, false);
    StructuredRecord structuredRecord = Mockito.mock(StructuredRecord.class);
    Mockito.when(structuredRecord.getSchema()).thenReturn(inputSchema);
    Mockito.when(structuredRecord.get("emailid")).thenReturn("abc@123");
    LocalDate date = LocalDate.of(2011, 01, 01);
    Mockito.when(structuredRecord.get("date")).thenReturn(date);
    Mockito.when(structuredRecord.getDate("date")).thenReturn(date);
    Mockito.when(structuredRecord.get("price")).thenReturn(2000);
    Mockito.when(structuredRecord.getDecimal("price")).thenReturn(BigDecimal.valueOf(2000));
    Assert.assertFalse(recordDataExtensionRowConverter.transform(structuredRecord).getColumn("price").isEmpty());
    Assert.assertFalse(recordDataExtensionRowConverter.transform(structuredRecord).getAllModified().isEmpty());

  }

  enum Days { Monday, Tuesday}
}


