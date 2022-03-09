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

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MarketingCloudInputSplitTest {

  @Test
  public void testInputSplitWithNonEmptyTableName() throws IOException, InterruptedException {
    MarketingCloudInputSplit actualMarketingCloudInputSplit = new MarketingCloudInputSplit("open Event", "Table Name");
    assertEquals(0L, actualMarketingCloudInputSplit.getLength());
    assertEquals("open Event", actualMarketingCloudInputSplit.getObjectName());
    assertEquals("Table Name", actualMarketingCloudInputSplit.getTableName());
  }

  @Test
  public void testInputSplitWithEmptyTableName() throws IOException, InterruptedException {
    MarketingCloudInputSplit actualMarketingCloudInputSplit = new MarketingCloudInputSplit();
    assertEquals(0L, actualMarketingCloudInputSplit.getLength());
    assertEquals(null, actualMarketingCloudInputSplit.getObjectName());
    Assert.assertNull(actualMarketingCloudInputSplit.getTableName());
  }

  @Test
  public void testReadFields() throws IOException {
    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit("open Event", "Table Name");
    ObjectInputStream objectInputStream = mock(ObjectInputStream.class);
    when(objectInputStream.readUTF()).thenReturn("Utf");
    marketingCloudInputSplit.readFields(objectInputStream);
    verify(objectInputStream, times(2)).readUTF();
    assertEquals("Utf", marketingCloudInputSplit.getTableName());
    assertEquals("Utf", marketingCloudInputSplit.getObjectName());

  }


  @Test
  public void testGetLocations() throws IOException, InterruptedException {
    assertEquals(0, (new MarketingCloudInputSplit("Open Event", "Table Name")).getLocations().length);
  }

  @Test
  public void testGetTableName() {
    String expectedValue = "Table";
    String tableName = "Table";
    String objectName = "Object Name";

    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit(objectName, tableName);
    String actualValue = marketingCloudInputSplit.getTableName();
    assertEquals(expectedValue, actualValue);


  }

  @Test
  public void testGetObjectName() {
    String tableName = "Table";
    String expectedValue = "Object Name";
    String objectName = "Object Name";


    MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit(objectName, tableName);
    String actualValue = marketingCloudInputSplit.getObjectName();

    Assert.assertFalse(marketingCloudInputSplit.getObjectName().isEmpty());
    assertEquals(expectedValue, actualValue);


  }

  @Test
  public void testWriteWithNullData() {
    try {

      DataOutput dataOutput = null;
      String tableName = "";
      String objectName = "";
      MarketingCloudInputSplit marketingCloudInputSplit = new MarketingCloudInputSplit(objectName, tableName);
      marketingCloudInputSplit.write(dataOutput);

    } catch (Exception exception) {
      exception.printStackTrace();
    }
  }
}
