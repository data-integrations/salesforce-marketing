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
package io.cdap.plugin.sfmc.source;

import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MarketingCloudObjectInfoTest {
  public static final SourceObject TEST_OBJECT = SourceObject.TRACKING_NOTSENT_EVENT;
  public static final String TEST_DATAEXTENSIONKEY = "test-dataExtensionKey";

  @Test
  public void testGetSchema() {
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);
    MarketingCloudObjectInfo info =
      new MarketingCloudObjectInfoBuilder().setObject(TEST_OBJECT).setColumns(columns)
        .setDataExtensionKey(TEST_DATAEXTENSIONKEY).build();
    Assert.assertFalse(info.getSchema().getFields().isEmpty());
  }

  @Test
  public void testValidateGetObject() {
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);

    MarketingCloudObjectInfo info =
      new MarketingCloudObjectInfoBuilder().setObject(TEST_OBJECT).setColumns(columns).
        setDataExtensionKey(TEST_DATAEXTENSIONKEY).build();
    Assert.assertEquals(SourceObject.TRACKING_NOTSENT_EVENT, info.getObject());
  }

  @Test
  public void testValidateGeTableName() {
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);
    String tableName = "notsent";
    MarketingCloudObjectInfo info =
      new MarketingCloudObjectInfoBuilder().setObject(TEST_OBJECT).setColumns(columns).
        setDataExtensionKey(TEST_DATAEXTENSIONKEY).build();
    Assert.assertEquals(tableName, info.getObject().getTableName());
  }

  @Test
  public void testValidateTableNameWithDataExtensionObject() {
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);
    String tableName = "dataextension";
    MarketingCloudObjectInfo info = new MarketingCloudObjectInfoBuilder().setObject(SourceObject.DATA_EXTENSION).
      setColumns(columns).
      setDataExtensionKey(TEST_DATAEXTENSIONKEY).build();
    Assert.assertEquals(tableName, info.getObject().getTableName());
  }

  @Test
  public void testFormattedTableName() {
    String tableName = "notsent";
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);
    MarketingCloudObjectInfo info = new MarketingCloudObjectInfo(TEST_OBJECT, TEST_DATAEXTENSIONKEY, columns);
    Assert.assertEquals(tableName, info.getFormattedTableName());

  }

  @Test
  public void testLookUpsFieldMaps() {
    String schemaFieldName = "schema";
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);
    MarketingCloudObjectInfo info = new MarketingCloudObjectInfo(TEST_OBJECT, TEST_DATAEXTENSIONKEY, columns);
    info.lookupFieldsMap(schemaFieldName);
  }
}

