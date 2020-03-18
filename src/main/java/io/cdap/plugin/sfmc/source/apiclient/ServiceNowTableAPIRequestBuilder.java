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

package io.cdap.plugin.sfmc.source.apiclient;

import com.google.common.base.Joiner;

import io.cdap.plugin.sfmc.source.util.SourceValueType;

import java.net.URLEncoder;
import java.util.Arrays;

/**
 * ServiceNowTableAPIRequestBuilder
 */
public class ServiceNowTableAPIRequestBuilder {
  private static final String TABLE_API_URL_TEMPLATE = "%s/api/now/table/%s";
/*
  public ServiceNowTableAPIRequestBuilder(String instanceBaseUrl, String tableName) {
    super(String.format(TABLE_API_URL_TEMPLATE, instanceBaseUrl, tableName));
  }

  public ServiceNowTableAPIRequestBuilder setQuery(String query) {
    try {
      this.parameters.put("sysparm_query", URLEncoder.encode(query, "UTF-8"));
    } catch (Exception e) {
    }
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setOffset(int offset) {
    this.parameters.put("sysparm_offset", String.valueOf(offset));
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setLimit(int limit) {
    this.parameters.put("sysparm_limit", String.valueOf(limit));
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setFields(String... fields) {
    if (fields == null || fields.length == 0) {
      return this;
    }

    try {
      this.parameters.put("sysparm_fields", URLEncoder.encode(Joiner.on(',').join(Arrays.asList(fields)), "UTF-8"));
    } catch (Exception e) {
    }
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setDisplayValue(SourceValueType displayValue) {
    this.parameters.put("sysparm_display_value", displayValue.getValue());
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setExcludeReferenceLink(boolean excludeRefLink) {
    this.parameters.put("sysparm_exclude_reference_link", String.valueOf(excludeRefLink));
    return this;
  }*/
}
