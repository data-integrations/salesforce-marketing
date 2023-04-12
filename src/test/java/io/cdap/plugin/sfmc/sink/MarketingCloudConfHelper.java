/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.exacttarget.fuelsdk.ETSdkException;

public class MarketingCloudConfHelper {

  /**
   * Utility class that provides handy methods to construct MarketingCloud Sink Config for testing
   */

  public static final String TEST_REF_NAME = "TestRefName";
  public static final String TEST_CLIENT_ID = "test-client-id";
  public static final String TEST_CLIENT_SECRET = "test-client-secret";
  public static final String TEST_SOAP_ENDPOINT = "TestSoapEndpoint";
  public static final String TEST_AUTH_ENDPOINT = "TestAuthEndpoint";

  public static ConfigBuilder newConfigBuilder() {
    return new ConfigBuilder();
  }

  public static class ConfigBuilder {
    private String referenceName = TEST_REF_NAME;
    private String clientId = TEST_CLIENT_ID;
    private String clientSecret = TEST_CLIENT_SECRET;
    private String authEndpoint = TEST_AUTH_ENDPOINT;
    private String soapEndpoint = TEST_SOAP_ENDPOINT;
    private String dataExtension;
    private Integer maxBatchSize;
    private String operation;
    private String columnMapping;
    private boolean failOnError;
    private boolean replaceWithSpaces;
    private boolean truncateText;

    public ConfigBuilder setFailOnError(boolean failOnError) {
      this.failOnError = failOnError;
      return this;
    }

    public ConfigBuilder setReplaceWithSpaces(boolean replaceWithSpaces) {
      this.replaceWithSpaces = replaceWithSpaces;
      return this;
    }

    public ConfigBuilder setTruncateText(boolean truncateText) {
      this.truncateText = truncateText;
      return this;
    }

    public ConfigBuilder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
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

    public ConfigBuilder setMaxBatchSize(Integer maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public ConfigBuilder setOperation(String operation) {
      this.operation = operation;
      return this;
    }

    public ConfigBuilder setDataExtension(String dataExtension) {
      this.dataExtension = dataExtension;
      return this;
    }

    public ConfigBuilder setColumnMapping(String columnMapping) {
      this.columnMapping = columnMapping;
      return this;
    }

    public MarketingCloudConf build() throws ETSdkException {
      return new MarketingCloudConf(referenceName, clientId, clientSecret, dataExtension, authEndpoint, soapEndpoint,
        maxBatchSize, operation, columnMapping, failOnError, replaceWithSpaces, truncateText);
    }
  }
}
