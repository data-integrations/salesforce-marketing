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

package io.cdap.plugin.sfmc.source.util;

import com.google.common.base.Strings;

/**
 * Information about a Salesforce table column.
 */
public class SalesforceColumn {
  private String fieldName;
  private String typeName;
  private String mandatory;

  public SalesforceColumn() {
  }

  /**
   * Constructor for SalesforceColumn.
   *
   * @param fieldName The column name
   * @param typeName  The data type name
   */
  public SalesforceColumn(String fieldName, String typeName) {
    this.fieldName = fieldName;
    this.typeName = typeName;
    this.mandatory = "false";
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getTypeName() {
    return typeName;
  }

  public boolean isMandatory() {
    return !Strings.isNullOrEmpty(mandatory) && "true".equalsIgnoreCase(mandatory);
  }
}
