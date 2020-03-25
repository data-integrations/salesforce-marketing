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

package io.cdap.plugin.sfmc.source;

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.sfmc.common.DataExtensionClient;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import io.cdap.plugin.sfmc.source.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_AUTH_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_CLIENT_ID;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_CLIENT_SECRET;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_DATA_EXTENSION_KEY;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_DATA_EXTENSION_KEY_LIST;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_QUERY_MODE;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_SOAP_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_TABLE_NAME_FIELD;

/**
 * Configuration for the {@link SalesforceSource}.
 */
public class SalesforceSourceConfig extends PluginConfig {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSourceConfig.class);

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private String referenceName;

  @Name(PROPERTY_QUERY_MODE)
  @Macro
  @Description("Mode of data retrieval. The mode can be one of two values: "
    + "`Multi Object` - will allow user to fetch data for multiple data extensions, "
    + "`Single Object` - will allow user to fetch data for single data extension.")
  private String queryMode;

  @Name(PROPERTY_DATA_EXTENSION_KEY_LIST)
  @Macro
  @Nullable
  @Description("Specify the data extension keys from which data to be fetched; for example: 'Key1,Key2'. " +
    "Note, this value will be ignored if the Mode is set to `Single Object`.")
  private String dataExtensionKeys;

  @Name(PROPERTY_TABLE_NAME_FIELD)
  @Macro
  @Nullable
  @Description("The name of the field that holds the data extension name. Must not be the name of any data " +
    "extension column that will be read. Defaults to `tablename`. Note, the Table name field value will be ignored " +
    "if the Mode is set to `Single Object`.")
  private String tableNameField;

  @Name(PROPERTY_DATA_EXTENSION_KEY)
  @Macro
  @Nullable
  @Description("Specify the data extension key from which data to be fetched. Note, this value will be ignored if " +
    "the Mode is set to `Multi Object`.")
  private String dataExtensionKey;

  @Name(PROPERTY_CLIENT_ID)
  @Macro
  @Description("OAuth2 client ID associated with an installed package in the Salesforce Marketing Cloud.")
  private String clientId;

  @Name(PROPERTY_CLIENT_SECRET)
  @Macro
  @Description("OAuth2 client secret associated with an installed package in the Salesforce Marketing Cloud.")
  private String clientSecret;

  @Name(PROPERTY_API_ENDPOINT)
  @Macro
  @Description("The REST API Base URL associated for the Server-to-Server API integration. " +
    "For example, https://instance.rest.marketingcloudapis.com/")
  private String restEndpoint;

  @Name(PROPERTY_AUTH_API_ENDPOINT)
  @Macro
  @Description("Authentication Base URL associated for the Server-to-Server API integration. " +
    "For example, https://instance.auth.marketingcloudapis.com/")
  private String authEndpoint;

  @Name(PROPERTY_SOAP_API_ENDPOINT)
  @Macro
  @Description("The SOAP Endpoint URL associated for the Server-to-Server API integration. " +
    "For example, https://instance.soap.marketingcloudapis.com/Service.asmx")
  private String soapEndpoint;

  public SalesforceSourceConfig(String referenceName, String queryMode, @Nullable String dataExtensionKeys,
                                @Nullable String tableNameField, @Nullable String dataExtensionKey, String clientId,
                                String clientSecret, String restEndpoint, String authEndpoint, String soapEndpoint) {
    this.referenceName = referenceName;
    this.queryMode = queryMode;
    this.dataExtensionKeys = dataExtensionKeys;
    this.tableNameField = tableNameField;
    this.dataExtensionKey = dataExtensionKey;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.restEndpoint = restEndpoint;
    this.authEndpoint = authEndpoint;
    this.soapEndpoint = soapEndpoint;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public SourceQueryMode getQueryMode(FailureCollector collector) {
    SourceQueryMode mode = getQueryMode();
    if (mode != null) {
      return mode;
    }

    collector.addFailure("Unsupported query mode value: " + queryMode,
      String.format("Supported modes are: %s", SourceQueryMode.getSupportedModes()))
      .withConfigProperty(PROPERTY_QUERY_MODE);
    throw collector.getOrThrowException();
  }

  public SourceQueryMode getQueryMode() {
    Optional<SourceQueryMode> sourceQueryMode = SourceQueryMode.fromValue(queryMode);

    return sourceQueryMode.isPresent() ? sourceQueryMode.get() : null;
  }

  @Nullable
  public String getDataExtensionKeys() {
    return dataExtensionKeys;
  }

  @Nullable
  public String getTableNameField() {
    return tableNameField;
  }

  @Nullable
  public String getDataExtensionKey() {
    return dataExtensionKey;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getRestEndpoint() {
    return restEndpoint;
  }

  public String getAuthEndpoint() {
    return authEndpoint;
  }

  public String getSoapEndpoint() {
    return soapEndpoint;
  }

  /**
   * Validates {@link SalesforceSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    //Validates the given referenceName to consists of characters allowed to represent a dataset.
    IdUtils.validateReferenceName(referenceName, collector);

    validateCredentials(collector);
    validateQueryMode(collector);
  }

  private void validateCredentials(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }

    if (Util.isNullOrEmpty(clientId)) {
      collector.addFailure("Client ID must be specified.", null)
        .withConfigProperty(PROPERTY_CLIENT_ID);
    }

    if (Util.isNullOrEmpty(clientSecret)) {
      collector.addFailure("Client Secret must be specified.", null)
        .withConfigProperty(PROPERTY_CLIENT_SECRET);
    }

    if (Util.isNullOrEmpty(restEndpoint)) {
      collector.addFailure(" REST Endpoint must be specified.", null)
        .withConfigProperty(PROPERTY_API_ENDPOINT);
    }

    if (Util.isNullOrEmpty(authEndpoint)) {
      collector.addFailure("Auth Endpoint  must be specified.", null)
        .withConfigProperty(PROPERTY_AUTH_API_ENDPOINT);
    }

    if (Util.isNullOrEmpty(soapEndpoint)) {
      collector.addFailure("Soap Endpoint must be specified.", null)
        .withConfigProperty(PROPERTY_SOAP_API_ENDPOINT);
    }

    try {
      DataExtensionClient.create("", clientId, clientSecret, authEndpoint, soapEndpoint);
    } catch (ETSdkException e) {
      collector.addFailure("Unable to connect to Salesforce Instance.",
        "Ensure properties like Client ID, Client Secret, API Endpoint, Soap Endpoint, Auth Endpoint " +
          "are correct.")
        .withConfigProperty(PROPERTY_CLIENT_ID)
        .withConfigProperty(PROPERTY_CLIENT_SECRET)
        .withConfigProperty(PROPERTY_API_ENDPOINT)
        .withConfigProperty(PROPERTY_AUTH_API_ENDPOINT)
        .withConfigProperty(PROPERTY_SOAP_API_ENDPOINT)
        .withStacktrace(e.getStackTrace());
    }
  }

  private void validateQueryMode(FailureCollector collector) {
    //according to query mode check if either table name/application exists or not
    if (containsMacro(PROPERTY_QUERY_MODE)) {
      return;
    }

    SourceQueryMode mode = getQueryMode(collector);

    if (mode == SourceQueryMode.MULTI_OBJECT) {
      validateMultiObjectQueryMode(collector);
    } else {
      validateSingleObjectQueryMode(collector);
    }
  }

  private void validateMultiObjectQueryMode(FailureCollector collector) {
    if (containsMacro(PROPERTY_DATA_EXTENSION_KEY_LIST) || containsMacro(PROPERTY_TABLE_NAME_FIELD)) {
      return;
    }

    List<String> dataExtensionKeyList = Util.splitToList(getDataExtensionKeys(), ',');
    if (dataExtensionKeyList.isEmpty()) {
      collector.addFailure("At least 1 Data Extension Key must be specified.", null)
        .withConfigProperty(PROPERTY_DATA_EXTENSION_KEY_LIST);
    }

    if (Util.isNullOrEmpty(tableNameField)) {
      collector.addFailure("Table name field must be specified.", null)
        .withConfigProperty(PROPERTY_TABLE_NAME_FIELD);
    }
  }

  private void validateSingleObjectQueryMode(FailureCollector collector) {
    if (containsMacro(PROPERTY_DATA_EXTENSION_KEY)) {
      return;
    }

    if (Util.isNullOrEmpty(dataExtensionKey)) {
      collector.addFailure("Data Extension Key must be specified.", null)
        .withConfigProperty(PROPERTY_DATA_EXTENSION_KEY);
    }
  }

  /**
   * Returns true if Salesforce can be connected to.
   */
  public boolean shouldConnect() {
    return !containsMacro(PROPERTY_CLIENT_ID) &&
      !containsMacro(PROPERTY_CLIENT_SECRET) &&
      !containsMacro(PROPERTY_API_ENDPOINT) &&
      !containsMacro(PROPERTY_AUTH_API_ENDPOINT) &&
      !containsMacro(PROPERTY_SOAP_API_ENDPOINT);
  }
}
