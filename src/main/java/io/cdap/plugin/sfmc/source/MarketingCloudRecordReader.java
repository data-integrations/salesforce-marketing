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
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Record reader that reads the entire contents of a Salesforce table.
 */
public class MarketingCloudRecordReader extends RecordReader<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudRecordReader.class);
  private final MarketingCloudSourceConfig pluginConf;
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
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);

    if (pluginConf.getQueryMode() == SourceQueryMode.MULTI_OBJECT) {
      recordBuilder.set(tableNameField, formattedTableName);
    }

    try {

      convertRecord(recordBuilder, row);

    } catch (Exception e) {
      LOG.error(String.format("Error decoding row from table %s", tableName), e);
      throw new IOException(String.format("Error decoding row from table %s", tableName), e);
    }
    return recordBuilder.build();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return pos / (float) split.getLength();
  }

  @Override
  public void close() throws IOException {
  }

  private void fetchData() throws Exception {
    object = SourceObject.valueOf(split.getObjectName());
    tableName = split.getTableName();
    formattedTableName = tableName.replaceAll("-", "_");

    if (object == SourceObject.DATA_EXTENSION) {
      dataExtensionKey = tableName.replaceAll(MarketingCloudConstants.DATA_EXTENSION_PREFIX, "");
    }
    tableNameField = pluginConf.getTableNameField();

    MarketingCloudClient client = MarketingCloudClient.create(pluginConf.getConnection().getClientId(),
                                                              pluginConf.getConnection().getClientSecret(),
                                                              pluginConf.getConnection().getAuthEndpoint(),
                                                              pluginConf.getConnection().getSoapEndpoint());
    //Fetch data
    if (object == SourceObject.DATA_EXTENSION) {
      response = client.fetchDataExtensionRecords(dataExtensionKey, object.getFilter(), requestId);
      results = response.getObjects();
      requestId = response.getRequestId();
    } else {
      response = client.fetchObjectRecords(object, requestId);
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
      fetchSchema(client);
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

  /**
   * Read data from ETApiObject and convert it to StructureRecord.
   */
  private void convertRecord(StructuredRecord.Builder recordBuilder, ETApiObject row) {
    for (Schema.Field field : tableFields) {
      String fieldName = field.getName();
      Object rawFieldValue = null;
      if (row instanceof ETDataExtensionRow) {
        String apiFieldName = sfObjectMetaData.lookupFieldsMap(fieldName);
        rawFieldValue = ((ETDataExtensionRow) row).getColumn(apiFieldName);
      } else {
        rawFieldValue = getFieldValue(row, fieldName);
      }

      Object fieldValue = convertToValue(fieldName, field.getSchema(), rawFieldValue);

      recordBuilder.set(fieldName, fieldValue);
    }
  }

  /**
   * Read data from ETApiObject using reflection for a given field name.
   */
  private Object getFieldValue(ETApiObject row, String fieldName) {
    try {
      Method method = row.getClass().getMethod(createGetterName(fieldName));
      return method.invoke(row);
    } catch (Exception e) {
      LOG.error(String.format("Error while fetching %s.%s value", row.getClass().getSimpleName(), fieldName), e);
      return null;
    }
  }

  /**
   * Constructs the get method name to be used in reflection call.
   */
  private String createGetterName(String name) {
    StringBuilder sb = new StringBuilder("get");
    sb.append(name.substring(0, 1).toUpperCase());
    sb.append(name.substring(1));
    return sb.toString();
  }

  /**
   * Converts raw field value according to the schema field type.
   */
  @VisibleForTesting
  Object convertToValue(String fieldName, Schema fieldSchema, Object fieldValue) {
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      return transformLogicalType(fieldName, logicalType, fieldValue);
    }

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

  @VisibleForTesting
  String convertToStringValue(Object fieldValue) {
    return String.valueOf(fieldValue);
  }

  @VisibleForTesting
  Double convertToDoubleValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Double.parseDouble(String.valueOf(fieldValue));
  }

  @VisibleForTesting
  Integer convertToIntegerValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Integer.parseInt(String.valueOf(fieldValue));
  }

  @VisibleForTesting
  Boolean convertToBooleanValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Boolean.parseBoolean(String.valueOf(fieldValue));
  }

  private Object transformLogicalType(String fieldName, Schema.LogicalType logicalType, Object value) {
    switch (logicalType) {
      case TIMESTAMP_MICROS:
        if (value instanceof Date) {
          return TimeUnit.MILLISECONDS.toMicros((((Date) value).getTime()));
        }
        return null;
      default:
        throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()));
    }
  }
}
