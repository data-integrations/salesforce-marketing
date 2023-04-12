/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.sfmc.source.util;

import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Salesforce Marketing Cloud Conversion
 */
public class MarketingCloudUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudUtil.class);

  //private static MarketingCloudClient client;

  private static Object getFieldValue(ETApiObject row, String fieldName) {
    try {
      Method method = row.getClass().getMethod(createGetterName(fieldName));
      return method.invoke(row);
    } catch (Exception e) {
      LOG.error(String.format("Error while fetching %s.%s value", row.getClass().getSimpleName(), fieldName), e);
      return null;
    }
  }

  private static String createGetterName(String name) {
    String sb = "get" + name.substring(0, 1).toUpperCase() +
      name.substring(1);
    return sb;
  }

  @VisibleForTesting
  public static String convertToStringValue(Object fieldValue) {
    return String.valueOf(fieldValue);
  }

  @VisibleForTesting
  public static Double convertToDoubleValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }
    return Double.parseDouble(String.valueOf(fieldValue));
  }

  @VisibleForTesting
  public static Integer convertToIntegerValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }
    return Integer.parseInt(String.valueOf(fieldValue));
  }

  @VisibleForTesting
  public static Boolean convertToBooleanValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }
    return Boolean.parseBoolean(String.valueOf(fieldValue));
  }

  @VisibleForTesting
  public static Object convertToValue(String fieldName, Schema fieldSchema, Object fieldValue) {
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

  public static void convertRecord(MarketingCloudObjectInfo sfObjectMetaData, StructuredRecord.Builder recordBuilder,
                                   ETApiObject row) {
    List<Schema.Field> tableFields = sfObjectMetaData.getSchema().getFields();
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

  private static Object transformLogicalType(String fieldName, Schema.LogicalType logicalType, Object value) {
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
