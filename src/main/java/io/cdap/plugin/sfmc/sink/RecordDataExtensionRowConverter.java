/*
 * Copyright © 2019 Cask Data, Inc.
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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Converts StructuredRecords into DataExtensionRows.
 */
public class RecordDataExtensionRowConverter {
  private final DataExtensionInfo dataExtensionInfo;
  private final DateTimeFormatter dateTimeFormatter;
  private final BiFunction<String, String, String> truncateFunction;

  public RecordDataExtensionRowConverter(DataExtensionInfo dataExtensionInfo, boolean shouldTruncate) {
    this.dataExtensionInfo = dataExtensionInfo;
    this.dateTimeFormatter = DateTimeFormatter.ofPattern("MM/DD/yyyy");
    this.truncateFunction = !shouldTruncate ? (fieldName, val) -> val :
      (fieldName, val) -> {
        ETDataExtensionColumn column = dataExtensionInfo.getColumn(fieldName);
        if (column == null || column.getLength() == null) {
          return val;
        }
        int maxLength = column.getLength();
        return val.length() > maxLength ? val.substring(0, maxLength) : val;
      };
  }

  public ETDataExtensionRow transform(StructuredRecord record) {
    ETDataExtensionRow row = new ETDataExtensionRow();
    row.setDataExtensionKey(dataExtensionInfo.getExternalKey());
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      if (!dataExtensionInfo.hasColumn(fieldName)) {
        continue;
      }

      Schema fieldSchema = field.getSchema();
      if (fieldSchema.isNullable()) {
        fieldSchema = fieldSchema.getNonNullable();
      }
      Object val = record.get(fieldName);
      if (val == null) {
        continue;
      }

      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null) {
        row.setColumn(fieldName, getLogicalType(logicalType, fieldName, record));
        continue;
      }

      Schema.Type fieldType = fieldSchema.getType();
      row.setColumn(fieldName, getType(fieldType, fieldName, val));
    }
    return row;
  }

  private String getLogicalType(Schema.LogicalType logicalType, String fieldName, StructuredRecord record) {
    switch (logicalType) {
      case DATE:
        LocalDate date = record.getDate(fieldName);
        return date.format(dateTimeFormatter);
      case DECIMAL:
        BigDecimal decimal = record.getDecimal(fieldName);
        return decimal.toString();
      default:
        // should never happen, as schema is supposed to be validated before this
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType));
    }
  }

  private String getType(Schema.Type type, String fieldName, Object val) {
    switch (type) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return String.valueOf(val);
      case STRING:
        String strVal = (String) val;
        return truncateFunction.apply(fieldName, strVal);
      case ENUM:
        return ((Enum) val).name();
      default:
        // should never happen, as schema is supposed to be validated before this
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, type));
    }
  }
}
