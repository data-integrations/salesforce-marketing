/*
 * Copyright © 2017 Cask Data, Inc.
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
 * Salesforce input split
 */
public class SalesforceInputSplit extends InputSplit implements Writable {
  private String tableKey;
  private String tableName;
  private int page;
  private int length;

  // used by mapreduce
  public SalesforceInputSplit() {
  }

  public SalesforceInputSplit(String tableKey, String tableName, int page, int length) {
    this.tableKey = tableKey;
    this.tableName = tableName;
    this.page = page;
    this.length = length;
  }

  public String getTableKey() {
    return tableKey;
  }

  public String getTableName() {
    return tableName;
  }

  public int getPage() {
    return page;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(this.tableKey);
    dataOutput.writeUTF(this.tableName);
    dataOutput.writeInt(this.page);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.tableKey = dataInput.readUTF();
    this.tableName = dataInput.readUTF();
    this.page = dataInput.readInt();
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return this.length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }
}
