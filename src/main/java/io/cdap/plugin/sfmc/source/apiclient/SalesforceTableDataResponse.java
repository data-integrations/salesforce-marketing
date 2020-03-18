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

package io.cdap.plugin.sfmc.source.apiclient;



import io.cdap.plugin.sfmc.source.util.SalesforceColumn;

import java.util.List;
import java.util.Map;

/**
 * A Pojo class to wrap the success response for Salesforce Table data
 */
public class SalesforceTableDataResponse {
  private int totalRecordCount;

  private List<SalesforceColumn> columns;

  private List<Map<String, Object>> result;

  public int getTotalRecordCount() {
    return totalRecordCount;
  }

  public void setTotalRecordCount(int totalRecordCount) {
    this.totalRecordCount = totalRecordCount;
  }

  public List<SalesforceColumn> getColumns() {
    return columns;
  }

  public void setColumns(List<SalesforceColumn> columns) {
    this.columns = columns;
  }

  public List<Map<String, Object>> getResult() {
    return result;
  }

  public void setResult(List<Map<String, Object>> result) {
    this.result = result;
  }
}
