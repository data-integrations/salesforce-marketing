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
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETResult;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataExtensionRecordWriterTest {

  @Test
  public void testWriteBatchWithInsert() throws ETSdkException, IOException {
    StructuredRecord record = Mockito.mock(StructuredRecord.class);
    ETResponse<ETDataExtensionRow> response = new ETResponse<>();
    response.setRequestId("id");
    response.setStatus(ETResult.Status.ERROR);
    List<ETDataExtensionRow> batch = new ArrayList<>();
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    ETDataExtensionColumn column = new ETDataExtensionColumn();
    column.setLength(123);
    column.setName("Name");
    columnList.add(column);
    DataExtensionClient dataExtensionClient = Mockito.mock(DataExtensionClient.class);
    Mockito.when(dataExtensionClient.insert(batch)).thenReturn(response);
    DataExtensionInfo info = new DataExtensionInfo("externalKey", columnList);
    RecordDataExtensionRowConverter recordDataExtensionRowConverter = new RecordDataExtensionRowConverter(info, true);
    DataExtensionRecordWriter dataExtensionRecordWriter = Mockito.spy(new DataExtensionRecordWriter
                                                                        (dataExtensionClient,
                                                                         recordDataExtensionRowConverter,
                                                                         Operation.INSERT, 0, false));
    dataExtensionRecordWriter.write(null, record);
    Mockito.verify(dataExtensionRecordWriter, Mockito.times(1)).writeBatch();
  }

  @Test
  public void testWriteBatchWithNullOperation() throws ETSdkException, IOException {
    StructuredRecord record = Mockito.mock(StructuredRecord.class);
    ETResponse<ETDataExtensionRow> response = new ETResponse<>();
    response.setRequestId("id");
    response.setStatus(ETResult.Status.ERROR);
    List<ETDataExtensionRow> batch = new ArrayList<>();
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    ETDataExtensionColumn column = new ETDataExtensionColumn();
    column.setLength(123);
    column.setName("Name");
    columnList.add(column);
    DataExtensionClient dataExtensionClient = Mockito.mock(DataExtensionClient.class);
    Mockito.when(dataExtensionClient.insert(batch)).thenReturn(response);
    DataExtensionInfo info = new DataExtensionInfo("externalKey", columnList);
    RecordDataExtensionRowConverter recordDataExtensionRowConverter = new RecordDataExtensionRowConverter(info, true);
    DataExtensionRecordWriter dataExtensionRecordWriter = Mockito.spy(new DataExtensionRecordWriter
                                                                        (dataExtensionClient,
                                                                         recordDataExtensionRowConverter, null, 0,
                                                                         true));
    try {
      dataExtensionRecordWriter.writeBatch();
    } catch (Exception e) {
      Mockito.verify(dataExtensionRecordWriter, Mockito.times(1)).writeBatch();
    }
  }

  @Test
  public void testWriteBatchWithUpsertInvokedByClose() throws ETSdkException, IOException {
    List<ETDataExtensionRow> batch = new ArrayList<>();
    List<ETResult<ETDataExtensionRow>> results = new ArrayList<>();
    TaskAttemptContext context = Mockito.mock(TaskAttemptContext.class);
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    ETDataExtensionColumn column = new ETDataExtensionColumn();
    column.setLength(123);
    column.setName("Name");
    columnList.add(column);
    DataExtensionClient dataExtensionClient = Mockito.mock(DataExtensionClient.class);
    Mockito.when(dataExtensionClient.upsert(batch)).thenReturn(results);
    DataExtensionInfo info = new DataExtensionInfo("externalKey", columnList);
    RecordDataExtensionRowConverter recordDataExtensionRowConverter = new RecordDataExtensionRowConverter(info, true);
    DataExtensionRecordWriter dataExtensionRecordWriter = Mockito.spy(new DataExtensionRecordWriter(dataExtensionClient,
      recordDataExtensionRowConverter, Operation.UPSERT, 1, false));
    dataExtensionRecordWriter.close(context);
    Mockito.verify(dataExtensionRecordWriter, Mockito.times(1)).writeBatch();
  }

  @Test
  public void testWriteBatchWithUpdateInvokedByClose() throws ETSdkException, IOException {
    TaskAttemptContext context = Mockito.mock(TaskAttemptContext.class);
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    ETDataExtensionColumn column = new ETDataExtensionColumn();
    column.setLength(123);
    column.setName("Name");
    columnList.add(column);
    DataExtensionClient client = Mockito.mock(DataExtensionClient.class);
    DataExtensionInfo info = new DataExtensionInfo("externalKey", columnList);
    ETResponse<ETDataExtensionRow> response = new ETResponse<>();
    response.setRequestId("id");
    List<ETDataExtensionRow> batch = new ArrayList<>();
    Mockito.when(client.update(batch)).thenReturn(response);
    RecordDataExtensionRowConverter recordDataExtensionRowConverter = new RecordDataExtensionRowConverter(info, false);
    DataExtensionRecordWriter dataExtensionRecordWriter = Mockito.spy(new DataExtensionRecordWriter(client,
      recordDataExtensionRowConverter, Operation.UPDATE, 500, false));
    dataExtensionRecordWriter.close(context);
    Mockito.verify(dataExtensionRecordWriter, Mockito.times(1)).writeBatch();
  }
}
