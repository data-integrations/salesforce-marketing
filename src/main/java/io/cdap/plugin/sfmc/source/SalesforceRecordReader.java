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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import io.cdap.plugin.sfmc.source.apiclient.SalesforceTableAPIClientImpl;
import io.cdap.plugin.sfmc.source.apiclient.SalesforceTableDataResponse;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PAGE_SIZE;

/**
 * Record reader that reads the entire contents of a ServiceNow table.
 */
public class SalesforceRecordReader extends RecordReader<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRecordReader.class);
  private final SalesforceSourceConfig pluginConf;
  private SalesforceInputSplit split;
  private int pos;
  private List<Schema.Field> tableFields;
  private Schema schema;

  private String tableName;

  private List<Map<String, Object>> results;
  private Iterator<Map<String, Object>> iterator;
  private Map<String, Object> row;

  SalesforceRecordReader(SalesforceSourceConfig pluginConf) {
    this.pluginConf = pluginConf;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    this.split = (SalesforceInputSplit) split;
    this.pos = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
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
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);

    if (pluginConf.getQueryMode() == SourceObjectMode.MULTIOBJECT) {

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
    tableName = split.getTableName();


    SalesforceTableAPIClientImpl restApi = new SalesforceTableAPIClientImpl(pluginConf);

    //Get the table data
   // results = restApi.fetchTableRecords(tableName, pluginConf.getStartDate(), pluginConf.getEndDate(),
    //  split.getOffset(), PAGE_SIZE);

    LOG.debug("size={}", results.size());
    if (!results.isEmpty()) {
      fetchSchema(restApi);
    }

    iterator = results.iterator();
  }

  private void fetchSchema(SalesforceTableAPIClientImpl restApi) {
    //Fetch the column definition
   // SalesforceTableDataResponse response = restApi.fetchTableSchema(tableName, null, null,
   //   false);
    SalesforceTableDataResponse response = null;
    if (response == null) {
      return;
    }

    //Build schema
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    Schema tempSchema = schemaBuilder.constructSchema(tableName, response.getColumns());
    tableFields = tempSchema.getFields();
    List<Schema.Field> schemaFields = new ArrayList<>(tableFields);

    if (pluginConf.getQueryMode() == SourceObjectMode.MULTIOBJECT) {

    }

    schema = Schema.recordOf(tableName, schemaFields);
  }

  private Object convertToValue(String fieldName, Schema fieldSchema, Map<String, Object> record) {
    Schema.Type fieldType = fieldSchema.getType();
    Object fieldValue = record.get(fieldName);

    switch (fieldType) {
      case STRING:
        return convertToStringValue(fieldValue);
      case DOUBLE:
        return convertToDoubleValue(fieldValue);
      case INT:
        return convertToIntegerValue(fieldValue);
      case BOOLEAN:
        return convertToBooleanValue(fieldValue);
      //case RECORD:
        //return convertToRecordValue(fieldName, fieldSchema, fieldValue);
        /*
        if (fieldValue instanceof Map) {
          Map<String, Object> nestedRecord = (Map<String, Object>) fieldValue;
          StructuredRecord.Builder nestedRecordBuilder = StructuredRecord.builder(fieldSchema);
          Objects.requireNonNull(fieldSchema.getFields(), "Nested Schema fields cannot be empty").forEach(
            nestedField -> {
              String nestedFieldName = nestedField.getName();
              Object nestedFieldValue = convertToValue(nestedFieldName, nestedField.getSchema(), nestedRecord);
              nestedRecordBuilder.set(nestedFieldName, nestedFieldValue);
            }
          );
          return nestedRecordBuilder.build();
        } else if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
          return null;
        } else {
          throw new IllegalStateException(
            String.format("Field '%s' is of unexpected type '%s'. Declared 'RECORD' types: %s",
              fieldName, record.get(fieldName).getClass().getSimpleName(), fieldSchema.toString()));
        }
        */
      case UNION:
        if (fieldSchema.isNullable()) {
          return convertToValue(fieldName, fieldSchema.getNonNullable(), record);
        }
        throw new IllegalStateException(
          String.format("Field '%s' is of unexpected type '%s'. Declared 'complex UNION' types: %s",
            fieldName, record.get(fieldName).getClass().getSimpleName(), fieldSchema.getUnionSchemas()));
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

  private StructuredRecord convertToRecordValue(String fieldName, Schema fieldSchema, Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    if (fieldValue instanceof Map) {
      Map<String, Object> nestedRecord = (Map<String, Object>) fieldValue;
      StructuredRecord.Builder nestedRecordBuilder = StructuredRecord.builder(fieldSchema);
      Objects.requireNonNull(fieldSchema.getFields(), "Nested Schema fields cannot be empty").forEach(
        nestedField -> {
          String nestedFieldName = nestedField.getName();
          Object nestedFieldValue = convertToValue(nestedFieldName, nestedField.getSchema(), nestedRecord);
          nestedRecordBuilder.set(nestedFieldName, nestedFieldValue);
        }
      );
      return nestedRecordBuilder.build();
    } else {
      throw new IllegalStateException(
        String.format("Field '%s' is of unexpected type '%s'. Declared 'RECORD' types: %s",
          fieldName, fieldValue.getClass().getSimpleName(), fieldSchema.toString()));
    }
  }
}
