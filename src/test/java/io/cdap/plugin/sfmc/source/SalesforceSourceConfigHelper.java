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

package io.cdap.plugin.sfmc.source;

/**
 * Utility class that provides handy methods to construct Salesforce Source Config for testing
 */
public class SalesforceSourceConfigHelper {

  public static final String TEST_REF_NAME = "TestRefName";
  public static final String TEST_CLIENT_ID = "test-client-id";
  public static final String TEST_CLIENT_SECRET = "test-client-secret";
  public static final String TEST_AUTH_ENDPOINT = "TestAuthEndpoint";
  public static final String TEST_SOAP_ENDPOINT = "TestSoapEndpoint";
  public static final String TEST_OBJECT_NAME = "Data Extension";
  public static final String TEST_OBJECT_LIST = "Data Extension";
  private static final String TEST_FILTER = "";

  public static ConfigBuilder newConfigBuilder() {
    return new ConfigBuilder();
  }

  public static class ConfigBuilder {
    private String referenceName = TEST_REF_NAME;
    private String clientId = TEST_CLIENT_ID;
    private String clientSecret = TEST_CLIENT_SECRET;
    private String authEndpoint = TEST_AUTH_ENDPOINT;
    private String soapEndpoint = TEST_SOAP_ENDPOINT;
    private String queryMode;
    private String objectName = TEST_OBJECT_NAME;
    private String filter = TEST_FILTER;
    private String dataExtensionKey;
    private String objectList = TEST_OBJECT_LIST;
    private String dataExtensionKeys;
    private String tableNameField = "tablename";

    public ConfigBuilder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public ConfigBuilder setQueryMode(String queryMode) {
      this.queryMode = queryMode;
      return this;
    }

    public ConfigBuilder setObjectName(String objectName) {
      this.objectName = objectName;
      return this;
    }

    public ConfigBuilder setFilter(String filter) {
      this.filter = filter;
      return this;
    }

    public ConfigBuilder setDataExtensionKey(String dataExtensionKey) {
      this.dataExtensionKey = dataExtensionKey;
      return this;
    }

    public ConfigBuilder setObjectList(String objectList) {
      this.objectList = objectList;
      return this;
    }

    public ConfigBuilder setDataExtensionKeys(String dataExtensionKeys) {
      this.dataExtensionKeys = dataExtensionKeys;
      return this;
    }

    public ConfigBuilder setTableNameField(String tableNameField) {
      this.tableNameField = tableNameField;
      return this;
    }

    public ConfigBuilder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public ConfigBuilder setClientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }


    public ConfigBuilder setAuthEndpoint(String authEndpoint) {
      this.authEndpoint = authEndpoint;
      return this;
    }

    public ConfigBuilder setSoapEndpoint(String soapEndpoint) {
      this.soapEndpoint = soapEndpoint;
      return this;
    }

    public MarketingCloudSourceConfig build() {
      return new MarketingCloudSourceConfig(referenceName, queryMode, objectName, dataExtensionKey, objectList,
                                            dataExtensionKeys, tableNameField, filter, clientId, clientSecret,
                                             authEndpoint, soapEndpoint);
    }
  }
}
