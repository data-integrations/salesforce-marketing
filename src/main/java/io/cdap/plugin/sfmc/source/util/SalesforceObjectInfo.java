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

package io.cdap.plugin.sfmc.source.util;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Information about a Salesforce table.
 */
public class SalesforceObjectInfo {
  private final String tableKey;
  private final String tableName;
  private final Schema schema;
  private final int recordCount;

  public SalesforceObjectInfo(String tableKey, String tableName, Schema schema, int recordCount) {
    this.tableKey = tableKey;
    this.tableName = tableName;
    this.schema = schema;
    this.recordCount = recordCount;
  }

  public String getTableKey() {
    return tableKey;
  }

  public String getTableName() {
    return tableName;
  }

  public Schema getSchema() {
    return schema;
  }

  public int getRecordCount() {
    return recordCount;
  }
}
