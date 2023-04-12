/*
 * Copyright © 2021 Cask Data, Inc.
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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Salesforce input split.
 */
public class MarketingCloudInputSplit extends InputSplit implements Writable {
  private String objectName;
  private String tableName;

  // used by mapreduce
  public MarketingCloudInputSplit() {
  }

  /**
   * Constructor for MarketingCloudInputSplit.
   *
   * @param objectName The object name
   * @param tableName  The corresponding table name
   */
  public MarketingCloudInputSplit(String objectName, String tableName) {
    this.objectName = objectName;
    this.tableName = tableName;
  }

  public String getObjectName() {
    return objectName;
  }

  public String getTableName() {
    return tableName;
  }


  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(this.objectName);
    dataOutput.writeUTF(this.tableName);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.objectName = dataInput.readUTF();
    this.tableName = dataInput.readUTF();
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }
}
