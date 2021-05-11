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

package io.cdap.plugin.sfmc.source.util;

/**
 * Information about a MarketingCloud table column.
 */
public class MarketingCloudColumn {
  private String fieldName;
  private String typeName;
  private String mandatory;

  public MarketingCloudColumn() {
  }

  /**
   * Constructor for MarketingCloudColumn.
   *
   * @param fieldName The column name
   * @param typeName  The data type name
   */
  public MarketingCloudColumn(String fieldName, String typeName) {
    this.fieldName = fieldName;
    this.typeName = typeName;
    this.mandatory = "false";
  }

  public String getFieldName() {
    return fieldName;
  }

  /**
   * Replaces all space characters in column name with double underscores
   *
   * @return field name with all characters replaced.
   */
  public String getFormattedFieldName() {
    return fieldName.replaceAll(" ", "__");
  }

  public String getTypeName() {
    return typeName;
  }

}
