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
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataExtensionInfoTest {
  public static final String TEST_EXTERNALKEY = "test-externalKey";

  @Test
  public void testValidateExternalKey() {
    DataExtensionInfo info = new DataExtensionInfoBuilder().setExternalKey(TEST_EXTERNALKEY).build();
    Assert.assertEquals(TEST_EXTERNALKEY, info.getExternalKey());
  }

  @Test
  public void testValidateColumnList() {
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
    DataExtensionInfo info =
      new DataExtensionInfoBuilder().setColumnList(columnList).setColumns(columns).setExternalKey(TEST_EXTERNALKEY).
        build();
    Assert.assertEquals(info.getColumnList().size(), 1);
    Assert.assertFalse(info.getColumnList().isEmpty());
  }

  @Test
  public void testHasColumns() {
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
    DataExtensionInfo info =
      new DataExtensionInfoBuilder().setColumnList(columnList).setColumns(columns).setExternalKey(TEST_EXTERNALKEY)
        .build();
    Assert.assertFalse(info.hasColumn("tableName"));
    Assert.assertTrue(info.hasColumn("name"));
  }

  @Test
  public void testGetColumn() {
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
    DataExtensionInfo info =
      new DataExtensionInfoBuilder().setColumnList(columnList).setColumns(columns).setExternalKey(TEST_EXTERNALKEY)
        .build();
    Assert.assertEquals("name", column.getName());
    Assert.assertEquals("key", info.getColumn("name").getKey());
  }
}
