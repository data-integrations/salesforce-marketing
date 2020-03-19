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
import io.cdap.plugin.sfmc.common.DataExtensionClient;
import io.cdap.plugin.sfmc.source.util.SourceObjectMode;
import io.cdap.plugin.sfmc.source.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.MAX_PAGE_SIZE;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_AUTH_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_CLIENT_ID;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_CLIENT_SECRET;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_DATA_EXTENSION_KEY;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_DATA_EXTENSION_KEY_LIST;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_PAGE_SIZE;
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
  @Description("Mode of query. The mode can be one of two values: "
    + "`Reporting` - will allow user to choose application for which data will be fetched for all tables, "
    + "`Table` - will allow user to enter table name for which data will be fetched.")
  private String queryMode;

  @Name(PROPERTY_DATA_EXTENSION_KEY_LIST)
  @Macro
  @Nullable
  @Description("Application name for which data to be fetched. The application can be one of three values: " +
    "`Contract Management` - will fetch data for all tables under Contract Management application, " +
    "`Product Catalog` - will fetch data for all tables under Product Catalog application, " +
    "`Procurement` - will fetch data for all tables under Procurement application. " +
    "Note, the Application name value will be ignored if the Mode is set to `Table`.")
  private String dataExtensionKeys;

  @Name(PROPERTY_TABLE_NAME_FIELD)
  @Macro
  @Nullable
  @Description("The name of the field that holds the table name. Must not be the name of any table column that " +
    "will be read. Defaults to `tablename`. Note, the Table name field value will be ignored if the Mode " +
    "is set to `Table`.")
  private String tableNameField;

  @Name(PROPERTY_DATA_EXTENSION_KEY)
  @Macro
  @Nullable
  @Description("The name of the Salesforce table from which data to be fetched. Note, the Table name value " +
    "will be ignored if the Mode is set to `Reporting`.")
  private String dataExtensionKey;

  @Name(PROPERTY_CLIENT_ID)
  @Macro
  @Description(" The Client ID for Salesforce Instance.")
  private String clientId;

  @Name(PROPERTY_CLIENT_SECRET)
  @Macro
  @Description("The Client Secret for Salesforce Instance.")
  private String clientSecret;

  @Name(PROPERTY_API_ENDPOINT)
  @Macro
  @Description("The REST API Endpoint for Salesforce Instance. For example, https://instance.service-now.com")
  private String restEndpoint;

  @Name(PROPERTY_AUTH_API_ENDPOINT)
  @Macro
  @Description("The AUTH API Endpoint for Salesforce Instance. " +
    "For example, https://instance.auth.marketingcloudapis.com")
  private String authEndpoint;

  @Name(PROPERTY_SOAP_API_ENDPOINT)
  @Macro
  @Description("The SOAP API Endpoint for Salesforce Instance. " +
    "For example https://instance.Salesforce.soap.marketingcloudapis.com/Service.asmx")
  private String soapEndpoint;

  @Name(PROPERTY_PAGE_SIZE)
  @Macro
  @Description("Maximum number of records that can be read from Salesforce Marketing Cloud in one page. "
    + "The minimum value is 1 and maximum value is 2500")
  private int pageSize;

  public SalesforceSourceConfig(String referenceName, String queryMode, @Nullable String dataExtensionKeys,
                                @Nullable String dataExtensionKey, String clientId, String clientSecret,
                                String restEndpoint, String authEndpoint, String soapEndpoint, int pageSize) {
    this.referenceName = referenceName;
    this.queryMode = queryMode;
    this.dataExtensionKeys = dataExtensionKeys;
    this.dataExtensionKey = dataExtensionKey;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.restEndpoint = restEndpoint;
    this.authEndpoint = authEndpoint;
    this.soapEndpoint = soapEndpoint;
    this.pageSize = pageSize;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public SourceObjectMode getQueryMode(FailureCollector collector) {
    SourceObjectMode mode = getQueryMode();
    if (mode != null) {
      return mode;
    }

    collector.addFailure("Unsupported query mode value: " + queryMode,
      String.format("Supported modes are: %s", SourceObjectMode.getSupportedModes()))
      .withConfigProperty(PROPERTY_QUERY_MODE);
    throw collector.getOrThrowException();
  }

  public SourceObjectMode getQueryMode() {
    Optional<SourceObjectMode> sourceQueryMode = SourceObjectMode.fromValue(queryMode);

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

  public int getPageSize() {
    return pageSize;
  }

  /**
   * Validates {@link SalesforceSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    //Validates the given referenceName to consists of characters allowed to represent a dataset.
    //IdUtils.validateReferenceName(referenceName, collector);

    validateCredentials(collector);
    validateQueryMode(collector);
    validatePageSize(collector);
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
      LOG.info("Called SalesforceTableAPIClientImpl");
      DataExtensionClient.create("", clientId, clientSecret, authEndpoint, soapEndpoint);
      LOG.info("Completed SalesforceTableAPIClientImpl");
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

    SourceObjectMode mode = getQueryMode(collector);

    if (mode == SourceObjectMode.MULTI_OBJECT) {
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

  private void validatePageSize(FailureCollector collector) {
    if (containsMacro(PROPERTY_PAGE_SIZE)) {
      return;
    }

    if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
      collector.addFailure(String.format("Invalid page size '%d'.", pageSize),
        String.format("Ensure the page size is at least 1 or at most '%d'", MAX_PAGE_SIZE))
        .withConfigProperty(PROPERTY_PAGE_SIZE);
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
