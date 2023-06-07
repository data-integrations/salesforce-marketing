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

import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSoapObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.MarketingCloudUtil;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Record reader that reads the entire contents of a Salesforce table.
 */
public class MarketingCloudRecordReader extends RecordReader<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudRecordReader.class);
  private final MarketingCloudSourceConfig pluginConf;
  SourceObject sourceObject;
  private MarketingCloudInputSplit split;
  private int pos;
  private List<Schema.Field> tableFields;
  private MarketingCloudObjectInfo sfObjectMetaData;
  private Schema schema;
  private SourceObject object;
  private String dataExtensionKey = "";
  private String tableName;
  private String formattedTableName;
  private String tableNameField;
  private List<? extends ETApiObject> results;
  private Iterator<? extends ETApiObject> iterator;
  private ETApiObject row;
  private ETResponse<? extends ETSoapObject> response;
  private boolean hasMoreRecords;
  private String requestId;

  MarketingCloudRecordReader(MarketingCloudSourceConfig pluginConf) {
    this.pluginConf = pluginConf;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    this.split = (MarketingCloudInputSplit) split;
    this.pos = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    try {
      if (results == null) {
        fetchData();
      }
      if (!iterator.hasNext()) {
        if (hasMoreRecords) {
          fetchData();
        } else {
          return false;
        }
      }
      row = iterator.next();
      pos++;
    } catch (Exception e) {
      LOG.error("Error communicating with Salesforce Marketing cloud. Check for transport errors", e);
      throw new IOException("Error communicating with Salesforce Marketing cloud. Check for transport errors", e);
    }
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException {
    MarketingCloudUtil cloudConversion = new MarketingCloudUtil();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    if (pluginConf.getQueryMode() == SourceQueryMode.MULTI_OBJECT) {
      recordBuilder.set(tableNameField, formattedTableName);
    }
    try {
      cloudConversion.convertRecord(sfObjectMetaData, recordBuilder, row);
    } catch (Exception e) {
      LOG.error(String.format("Error decoding row from table %s", tableName), e);
      throw new IOException(String.format("Error decoding row from table %s", tableName), e);
    }
    return recordBuilder.build();
  }

  @Override
  public float getProgress() {
    return pos / (float) split.getLength();
  }

  @Override
  public void close() {
  }

  private void fetchData() throws Exception {
    object = SourceObject.valueOf(split.getObjectName());
    tableName = split.getTableName();
    formattedTableName = tableName.replaceAll("-", "_");

    if (object == SourceObject.DATA_EXTENSION) {
      dataExtensionKey = tableName.replaceAll(MarketingCloudConstants.DATA_EXTENSION_PREFIX, "");
    }
    tableNameField = pluginConf.getTableNameField();
    MarketingCloudClient marketingCloudInputFormat =
      MarketingCloudClient.getOrCreate(pluginConf.getConnection().getClientId(),
                                       pluginConf.getConnection().getClientSecret(),
                                       pluginConf.getConnection().getAuthEndpoint(),
                                       pluginConf.getConnection().getSoapEndpoint());
    String filterStr = pluginConf.getFilter();
    //Fetch data
    if (object == SourceObject.DATA_EXTENSION) {
      response = marketingCloudInputFormat.fetchDataExtensionRecords(dataExtensionKey, filterStr, requestId);
      results = response.getObjects();
      requestId = response.getRequestId();
    } else {
      response = marketingCloudInputFormat.fetchObjectRecords(object, filterStr, requestId);
      results = response.getObjects();
      requestId = response.getRequestId();
    }
    if (response.getResponseMessage().equals("MoreDataAvailable")) {
      hasMoreRecords = true;
    } else {
      hasMoreRecords = false;
    }
    LOG.debug("size={}", results.size());
    if (!results.isEmpty()) {
      fetchSchema(marketingCloudInputFormat);
    }
    iterator = results.iterator();
  }

  private void fetchSchema(MarketingCloudClient client) {
    //Fetch the column definition
    List<Schema.Field> schemaFields;
    try {
      if (object == SourceObject.DATA_EXTENSION) {
        sfObjectMetaData = client.fetchDataExtensionSchema(dataExtensionKey);
      } else {
        sfObjectMetaData = client.fetchObjectSchema(object);
      }
      //Build schema
      tableFields = sfObjectMetaData.getSchema().getFields();
      schemaFields = new ArrayList<>(tableFields);
      if (pluginConf.getQueryMode() == SourceQueryMode.MULTI_OBJECT) {
        schemaFields.add(Schema.Field.of(tableNameField, Schema.of(Schema.Type.STRING)));
      }
    } catch (Exception e) {
      schemaFields = Collections.emptyList();
    }
    schema = Schema.recordOf(tableName.replaceAll("-", "_"), schemaFields);
  }
}
