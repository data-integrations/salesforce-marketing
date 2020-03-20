/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sfmc.common.DataExtensionClient;
import io.cdap.plugin.sfmc.common.DataExtensionInfo;
import io.cdap.plugin.sfmc.source.util.SalesforceColumn;
import io.cdap.plugin.sfmc.source.util.SchemaBuilder;
import io.cdap.plugin.sfmc.source.util.SourceObjectMode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Record reader that reads the entire contents of a Salesforce table.
 */
public class SalesforceRecordReader extends RecordReader<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRecordReader.class);
  private final SalesforceSourceConfig pluginConf;
  private SalesforceInputSplit split;
  private int pos;
  private List<Schema.Field> tableFields;
  private Schema schema;

  private String tableKey;
  private String tableName;
  private String tableNameField;
  private List<ETDataExtensionRow> results;
  private Iterator<ETDataExtensionRow> iterator;
  private ETDataExtensionRow row;

  SalesforceRecordReader(SalesforceSourceConfig pluginConf) {
    this.pluginConf = pluginConf;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    LOG.error("In initialize()");
    this.split = (SalesforceInputSplit) split;
    this.pos = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    LOG.error("In nextKeyValue()");
    try {
      if (results == null) {
        fetchData();
      }

      if (!iterator.hasNext()) {
        return false;
      }

      row = iterator.next();

      pos++;
    } catch (Exception e) {
      LOG.error("Error in nextKeyValue", e);
      throw new IOException("Exception in nextKeyValue", e);
    }
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException {
    LOG.error("In getCurrentValue()");
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);

    if (pluginConf.getQueryMode() == SourceObjectMode.MULTI_OBJECT) {
      recordBuilder.set(tableNameField, tableName);
    }

    try {
      for (Schema.Field field : tableFields) {
        String fieldName = field.getName();
        Object fieldValue = convertToValue(fieldName, field.getSchema(), row);
        recordBuilder.set(fieldName, fieldValue);
      }
    } catch (Exception e) {
      LOG.error("Error decoding row from table " + tableName, e);
      throw new IOException("Error decoding row from table " + tableName, e);
    }
    return recordBuilder.build();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return pos / (float) split.getLength();
  }

  @Override
  public void close() throws IOException {
    SQLException exception = null;

    if (exception != null) {
      throw new IOException(exception);
    }
  }

  private void fetchData() {
    LOG.error("In fetchData()");
    tableKey = split.getTableKey();
    tableName = split.getTableName();
    tableNameField = pluginConf.getTableNameField();
    LOG.error("In fetchData(), tableKey={}, tableName={}", tableKey, tableName);

    //SalesforceTableAPIClientImpl restApi = new SalesforceTableAPIClientImpl(pluginConf);

    //Get the table data
    // results = restApi.fetchTableRecords(tableName, pluginConf.getStartDate(), pluginConf.getEndDate(),
    //  split.getOffset(), PAGE_SIZE);

    try {
      DataExtensionClient client = DataExtensionClient.create(tableKey, pluginConf.getClientId(),
        pluginConf.getClientSecret(), pluginConf.getAuthEndpoint(), pluginConf.getSoapEndpoint());

      results = client.pagedScan(split.getPage(), pluginConf.getPageSize());

      LOG.error("size={}", results.size());
      if (!results.isEmpty()) {
        fetchSchema(client);
      }

    } catch (Exception e) {
      results = Collections.emptyList();
      LOG.error("Error while fetching data", e);
    }

    iterator = results.iterator();
  }

  private void fetchSchema(DataExtensionClient client) {
    //Fetch the column definition
    // SalesforceTableDataResponse response = restApi.fetchTableSchema(tableName, null, null,
    //   false);
    /*
    SalesforceTableDataResponse response = null;
    if (response == null) {
      return;
    }
    */
    LOG.error("In fetchSchema()");

    List<Schema.Field> schemaFields;

    try {
      DataExtensionInfo metaData = client.getDataExtensionInfo();

      List<SalesforceColumn> columns = metaData.getColumnList().stream()
        .map(o -> new SalesforceColumn(o.getName(), o.getType().name()))
        .collect(Collectors.toList());

      //Build schema
      SchemaBuilder schemaBuilder = new SchemaBuilder();
      Schema tempSchema = schemaBuilder.constructSchema(tableName, columns);
      tableFields = tempSchema.getFields();
      schemaFields = new ArrayList<>(tableFields);

      if (pluginConf.getQueryMode() == SourceObjectMode.MULTI_OBJECT) {
        schemaFields.add(Schema.Field.of(tableNameField, Schema.of(Schema.Type.STRING)));
      }

    } catch (Exception e) {
      schemaFields = Collections.emptyList();
    }

    schema = Schema.recordOf(tableName, schemaFields);
  }

  private Object convertToValue(String fieldName, Schema fieldSchema, ETDataExtensionRow record) {
    Schema.Type fieldType = fieldSchema.getType();
    Object fieldValue = record.getColumn(fieldName);

    switch (fieldType) {
      case STRING:
        return convertToStringValue(fieldValue);
      case DOUBLE:
        return convertToDoubleValue(fieldValue);
      case INT:
        return convertToIntegerValue(fieldValue);
      case BOOLEAN:
        return convertToBooleanValue(fieldValue);
      case UNION:
        if (fieldSchema.isNullable()) {
          return convertToValue(fieldName, fieldSchema.getNonNullable(), record);
        }
        throw new IllegalStateException(
          String.format("Field '%s' is of unexpected type '%s'. Declared 'complex UNION' types: %s",
            fieldName, record.getColumn(fieldName).getClass().getSimpleName(), fieldSchema.getUnionSchemas()));
      default:
        throw new IllegalStateException(
          String.format("Record type '%s' is not supported for field '%s'", fieldType.name(), fieldName));
    }
  }

  private String convertToStringValue(Object fieldValue) {
    return String.valueOf(fieldValue);
  }

  private Double convertToDoubleValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Double.parseDouble(String.valueOf(fieldValue));
  }

  private Integer convertToIntegerValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Integer.parseInt(String.valueOf(fieldValue));
  }

  private Boolean convertToBooleanValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Boolean.parseBoolean(String.valueOf(fieldValue));
  }
}
