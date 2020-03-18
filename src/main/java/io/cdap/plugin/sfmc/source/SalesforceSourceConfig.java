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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.sfmc.source.apiclient.SalesforceTableAPIClientImpl;
import io.cdap.plugin.sfmc.source.util.SourceApplication;
import io.cdap.plugin.sfmc.source.util.SourceObjectMode;
import io.cdap.plugin.sfmc.source.util.SourceValueType;
import io.cdap.plugin.sfmc.source.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import javax.annotation.Nullable;

import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.DATE_FORMAT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_AUTH_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_CLIENT_ID;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_CLIENT_SECRET;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_END_DATE;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_MULTIOBJECT_NAME;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_OBJECT_NAME;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_QUERY_MODE;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_SOAP_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_START_DATE;



import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PROPERTY_VALUE_TYPE;
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

  @Name(PROPERTY_MULTIOBJECT_NAME)
  @Macro
  @Nullable
  @Description("Application name for which data to be fetched. The application can be one of three values: " +
    "`Contract Management` - will fetch data for all tables under Contract Management application, " +
    "`Product Catalog` - will fetch data for all tables under Product Catalog application, " +
    "`Procurement` - will fetch data for all tables under Procurement application. " +
    "Note, the Application name value will be ignored if the Mode is set to `Table`.")
  private String multiObjectName;

  @Name(PROPERTY_OBJECT_NAME)
  @Macro
  @Nullable
  @Description("The name of the Salesforce table from which data to be fetched. Note, the Table name value " +
    "will be ignored if the Mode is set to `Reporting`.")
  private String objectName;

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

  @Name(PROPERTY_VALUE_TYPE)
  @Macro
  @Nullable
  @Description("The type of values to be returned. The type can be one of two values: "
    + "`Actual` -  will fetch the actual values from the Salesforce tables, "
    + "`Display` - will fetch the display values from the Salesforce tables.")
  private String valueType;

  @Name(PROPERTY_START_DATE)
  @Macro
  @Nullable
  @Description("The Start date to be used to filter the data. The format must be 'yyyy-MM-dd'.")
  private String startDate;

  @Name(PROPERTY_END_DATE)
  @Macro
  @Nullable
  @Description("The End date to be used to filter the data. The format must be 'yyyy-MM-dd'.")
  private String endDate;

  public SalesforceSourceConfig(String referenceName, String queryMode, @Nullable String multiObjectName,
                                @Nullable String objectName, String clientId,
                                String clientSecret, String restEndpoint, String authEndpoint, String soapEndpoint,
                                @Nullable String valueType, @Nullable String startDate, @Nullable String endDate) {
    this.referenceName = referenceName;
    this.queryMode = queryMode;
    this.multiObjectName = multiObjectName;

    this.objectName = objectName;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.restEndpoint = restEndpoint;
    this.authEndpoint = authEndpoint;
    this.soapEndpoint = soapEndpoint;
    this.valueType = valueType;
    this.startDate = startDate;
    this.endDate = endDate;
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

  public SourceApplication getApplicationName(FailureCollector collector) {
    SourceApplication application = getApplicationName();
    if (application != null) {
      return application;
    }

    collector.addFailure("Unsupported multiObject name value: " + multiObjectName,
      String.format("Supported applications are: %s", SourceApplication.getSupportedApplications()))
      .withConfigProperty(PROPERTY_MULTIOBJECT_NAME);
    throw collector.getOrThrowException();
  }

  @Nullable
  public SourceApplication getApplicationName() {
    Optional<SourceApplication> sourceApplication = SourceApplication.fromValue(multiObjectName);

    return sourceApplication.isPresent() ? sourceApplication.get() : null;
  }



  @Nullable
  public String getObjectName() {
    return objectName;
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

  public SourceValueType getValueType(FailureCollector collector) {
    SourceValueType type = getValueType();
    if (type != null) {
      return type;
    }

    collector.addFailure("Unsupported type value: " + valueType,
      String.format("Supported value types are: %s", SourceValueType.getSupportedValueTypes()))
      .withConfigProperty(PROPERTY_VALUE_TYPE);
    throw collector.getOrThrowException();
  }

  @Nullable
  public SourceValueType getValueType() {
    Optional<SourceValueType> sourceValueType = SourceValueType.fromValue(valueType);

    return sourceValueType.isPresent() ? sourceValueType.get() : null;
  }

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
  }

  /**
   * Validates {@link SalesforceSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    //Validates the given referenceName to consists of characters allowed to represent a dataset.
    //IdUtils.validateReferenceName(referenceName, collector);

    validateCredentials(collector);
    validateQueryMode(collector);
    getValueType(collector);
    validateDateRange(collector);
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
      SalesforceTableAPIClientImpl restApi = new SalesforceTableAPIClientImpl(this);
      LOG.info("Completed SalesforceTableAPIClientImpl");
   //   restApi.getAccessToken();
      //1- TODO
    } catch (Exception e) {
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

    if (mode == SourceObjectMode.MULTIOBJECT) {
      validateMultiObjectQueryMode(collector);
    } else {
      validateObjectQueryMode(collector);
    }
  }

  private void validateMultiObjectQueryMode(FailureCollector collector) {
    if (!containsMacro(PROPERTY_MULTIOBJECT_NAME)) {
      getApplicationName(collector);
    }




  }

  private void validateObjectQueryMode(FailureCollector collector) {
    if (containsMacro(PROPERTY_OBJECT_NAME)) {
      return;
    }

    if (Util.isNullOrEmpty(objectName)) {
      collector.addFailure("Object name must be specified.", null)
        .withConfigProperty(PROPERTY_OBJECT_NAME);
    }
  }

  private void validateDateRange(FailureCollector collector) {
    if (containsMacro(PROPERTY_START_DATE) || containsMacro(PROPERTY_END_DATE)) {
      return;
    }

    if (Util.isNullOrEmpty(startDate) || Util.isNullOrEmpty(endDate)) {
      return;
    }

    //validate the date formats for both start date & end date
    if (!Util.isValidDateFormat(DATE_FORMAT, startDate)) {
      collector.addFailure("Invalid format for Start date. Correct Format: " + DATE_FORMAT, null)
        .withConfigProperty(PROPERTY_START_DATE);
    }

    if (!Util.isValidDateFormat(DATE_FORMAT, endDate)) {
      collector.addFailure("Invalid format for End date. Correct Format:" + DATE_FORMAT, null)
        .withConfigProperty(PROPERTY_END_DATE);
    }

    //validate the date range by checking if start date is smaller than end date
    LocalDate fromDate = LocalDate.parse(startDate);
    LocalDate toDate = LocalDate.parse(endDate);
    long noOfDays = ChronoUnit.DAYS.between(fromDate, toDate);

    if (noOfDays < 0) {
      collector.addFailure("End date must be greater than Start date.", null)
        .withConfigProperty(PROPERTY_START_DATE)
        .withConfigProperty(PROPERTY_END_DATE);
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
