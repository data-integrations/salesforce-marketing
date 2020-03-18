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
 * ServiceNow input split
 */
public class SalesforceInputSplit extends InputSplit implements Writable {
  private String tableName;
  private int offset;
  private int length;

  // used by mapreduce
  public SalesforceInputSplit() {
  }

  public SalesforceInputSplit(String tableName, int offset, int length) {
    this.tableName = tableName;
    this.offset = offset;
    this.length = length;
  }

  public String getTableName() {
    return tableName;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(this.tableName);
    dataOutput.writeInt(this.offset);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.tableName = dataInput.readUTF();
    this.offset = dataInput.readInt();
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
