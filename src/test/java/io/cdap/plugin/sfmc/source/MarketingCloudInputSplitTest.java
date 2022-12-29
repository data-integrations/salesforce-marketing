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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

public class MarketingCloudInputSplitTest {

  @Test
  public void testInputSplitWithNonEmptyTableName() {
    MarketingCloudInputSplit actualMarketingCloudInputSplit = new MarketingCloudInputSplit("open Event", "Table Name");
    Assert.assertEquals(0L, actualMarketingCloudInputSplit.getLength());
    Assert.assertEquals("open Event", actualMarketingCloudInputSplit.getObjectName());
    Assert.assertEquals("Table Name", actualMarketingCloudInputSplit.getTableName());
  }

  @Test
  public void testInputSplitWithEmptyTableName() {
    MarketingCloudInputSplit actualMarketingCloudInputSplit = new MarketingCloudInputSplit();
    Assert.assertEquals(0L, actualMarketingCloudInputSplit.getLength());
    Assert.assertNull(actualMarketingCloudInputSplit.getObjectName());
    Assert.assertNull(actualMarketingCloudInputSplit.getTableName());
  }

  @Test
  public void testReadFields() throws IOException {
    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit("open Event", "Table Name");
    ObjectInputStream objectInputStream = Mockito.mock(ObjectInputStream.class);
    Mockito.when(objectInputStream.readUTF()).thenReturn("Utf");
    marketingCloudInputSplit.readFields(objectInputStream);
    Assert.assertEquals("Utf", marketingCloudInputSplit.getTableName());
    Assert.assertEquals("Utf", marketingCloudInputSplit.getObjectName());
  }

  @Test
  public void testWrite() throws IOException {
    MarketingCloudInputSplit marketingCloudInputSplit = Mockito.spy(new MarketingCloudInputSplit("open Event"
      , "Table Name"));
    DataOutput dataOutput = Mockito.mock(DataOutput.class);
    marketingCloudInputSplit.write(dataOutput);
    Mockito.verify(marketingCloudInputSplit, Mockito.times(1)).write(dataOutput);
  }

  @Test
  public void testGetLocations() {
    Assert.assertEquals(String[].class, new MarketingCloudInputSplit("Table Name", "Object Name").getLocations().
      getClass());
    Assert.assertEquals(0, (new MarketingCloudInputSplit("Table Name", "Object Name")).getLocations().length);
  }

  @Test
  public void testGetTableName() {
    String expectedValue = "Table";
    String tableName = "Table";
    String objectName = "Object Name";
    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit(objectName, tableName);
    String actualValue = marketingCloudInputSplit.getTableName();
    Assert.assertEquals(expectedValue, actualValue);
  }

  @Test
  public void testGetObjectName() {
    String tableName = "Table";
    String expectedValue = "Object Name";
    String objectName = "Object Name";
    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit(objectName, tableName);
    String actualValue = marketingCloudInputSplit.getObjectName();
    Assert.assertFalse(marketingCloudInputSplit.getObjectName().isEmpty());
    Assert.assertEquals(expectedValue, actualValue);
  }

  @Test(expected = NullPointerException.class)
  public void testWriteWithNullData() throws IOException {
    DataOutput dataOutput = null;
    String tableName = "";
    String objectName = "";
    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit(tableName, objectName);
    marketingCloudInputSplit.write(dataOutput);
  }

  @Test(expected = NullPointerException.class)
  public void testRead() throws IOException {
    DataInput dataInput = null;
    String tableName = "";
    String objectName = "";
    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit(objectName, tableName);
    marketingCloudInputSplit.readFields(dataInput);
  }
}
