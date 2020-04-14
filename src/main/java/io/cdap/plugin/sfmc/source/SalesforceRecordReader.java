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

import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sfmc.source.util.SalesforceObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.DATA_EXTENSION_PREFIX;

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

  private SourceObject object;
  private String dataExtensionKey = "";
  private String tableName;
  private String tableNameField;
  private List<? extends ETApiObject> results;
  private Iterator<? extends ETApiObject> iterator;
  private ETApiObject row;

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
      if (pluginConf.isFailOnError()) {
        LOG.error("Error in nextKeyValue", e);
        throw new IOException("Exception in nextKeyValue", e);
      } else {
        LOG.warn("Failed in nextKeyValue", e);
      }
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

    if (pluginConf.getQueryMode() == SourceQueryMode.MULTI_OBJECT) {
      recordBuilder.set(tableNameField, tableName);
    }

    try {

      convertRecord(recordBuilder, row);

    } catch (Exception e) {
      if (pluginConf.isFailOnError()) {
        LOG.error("Error decoding row from table " + tableName, e);
        throw new IOException("Error decoding row from table " + tableName, e);
      } else {
        LOG.warn("Failed decoding row from table " + tableName, e);
      }
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
    object = SourceObject.valueOf(split.getObjectName());
    tableName = split.getTableName();
    if (object == SourceObject.DATA_EXTENSION) {
      dataExtensionKey = tableName.replaceAll(DATA_EXTENSION_PREFIX, "");
    }
    tableNameField = pluginConf.getTableNameField();

    //Get the table data
    try {
      SalesforceClient client = SalesforceClient.create(pluginConf.getClientId(),
        pluginConf.getClientSecret(), pluginConf.getAuthEndpoint(), pluginConf.getSoapEndpoint());

      //Fetch data
      if (object == SourceObject.DATA_EXTENSION) {
        results = client.fetchDataExtensionRecords(dataExtensionKey);
      } else {
        results = client.fetchObjectRecords(object);
      }

      LOG.debug("size={}", results.size());
      if (!results.isEmpty()) {
        fetchSchema(client);
      }
    } catch (Exception e) {
      results = Collections.emptyList();
      if (pluginConf.isFailOnError()) {
        LOG.error("Error while fetching data", e);
      } else {
        LOG.warn("Failed while fetching data", e);
      }
    }

    iterator = results.iterator();
  }

  private void fetchSchema(SalesforceClient client) {
    //Fetch the column definition
    List<Schema.Field> schemaFields;

    try {
      SalesforceObjectInfo metaData = null;
      if (object == SourceObject.DATA_EXTENSION) {
        metaData = client.fetchDataExtensionSchema(dataExtensionKey);
      } else {
        metaData = client.fetchObjectSchema(object);
      }

      //Build schema
      tableFields = metaData.getSchema().getFields();
      schemaFields = new ArrayList<>(tableFields);

      if (pluginConf.getQueryMode() == SourceQueryMode.MULTI_OBJECT) {
        schemaFields.add(Schema.Field.of(tableNameField, Schema.of(Schema.Type.STRING)));
      }
    } catch (Exception e) {
      schemaFields = Collections.emptyList();
    }

    schema = Schema.recordOf(tableName, schemaFields);
  }

  /**
   * Read data from ETApiObject and convert it to StructureRecord
   */
  private void convertRecord(StructuredRecord.Builder recordBuilder, ETApiObject row) {
    for (Schema.Field field : tableFields) {
      String fieldName = field.getName();
      Object rawFieldValue = null;
      if (row instanceof ETDataExtensionRow) {
        rawFieldValue = ((ETDataExtensionRow) row).getColumn(fieldName);
      } else {
        rawFieldValue = getFieldValue(row, fieldName);
      }

      Object fieldValue = convertToValue(fieldName, field.getSchema(), rawFieldValue);

      recordBuilder.set(fieldName, fieldValue);
    }
  }

  /**
   * Read data from ETApiObject using reflection for a given field name
   */
  private Object getFieldValue(ETApiObject row, String fieldName) {
    try {
      Method method = row.getClass().getMethod(createGetterName(fieldName));
      return method.invoke(row);
    } catch (Exception e) {
      if (pluginConf.isFailOnError()) {
        LOG.error(String.format("Error while fetching %s.%s value", row.getClass().getSimpleName(), fieldName), e);
      } else {
        LOG.warn(String.format("Failed while fetching %s.%s value", row.getClass().getSimpleName(), fieldName), e);
      }
      return null;
    }
  }

  /**
   * Constructs the get method name to be used in reflection call
   */
  private String createGetterName(String name) {
    StringBuilder sb = new StringBuilder("get");
    sb.append(name.substring(0, 1).toUpperCase());
    sb.append(name.substring(1));
    return sb.toString();
  }

  /**
   * Converts raw field value according to the schema field type
   */
  private Object convertToValue(String fieldName, Schema fieldSchema, Object fieldValue) {
    Schema.Type fieldType = fieldSchema.getType();

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
          return convertToValue(fieldName, fieldSchema.getNonNullable(), fieldValue);
        }
        throw new IllegalStateException(
          String.format("Field '%s' is of unexpected type '%s'. Declared 'complex UNION' types: %s",
            fieldName, fieldValue.getClass().getSimpleName(), fieldSchema.getUnionSchemas()));
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
