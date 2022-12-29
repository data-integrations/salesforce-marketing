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

package io.cdap.plugin.sfmc.connector;

import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETSdkException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import io.cdap.plugin.sfmc.source.MarketingCloudClient;
import io.cdap.plugin.sfmc.source.MarketingCloudInputFormat;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Salesforce Marketing Cloud Connector Config
 */
public class MarketingConnectorConfig extends PluginConfig {
  @Name(MarketingCloudConstants.PROPERTY_CLIENT_ID)
  @Macro
  @Description("OAuth2 client ID associated with an installed package in the Salesforce Marketing Cloud.")
  private final String clientId;

  @Name(MarketingCloudConstants.PROPERTY_CLIENT_SECRET)
  @Macro
  @Description("OAuth2 client secret associated with an installed package in the Salesforce Marketing Cloud.")
  private final String clientSecret;

  @Name(MarketingCloudConstants.PROPERTY_AUTH_API_ENDPOINT)
  @Macro
  @Description("Authentication Base URL associated for the Server-to-Server API integration. " +
          "For example, https://instance.auth.marketingcloudapis.com/")
  private final String authEndpoint;

  @Name(MarketingCloudConstants.PROPERTY_SOAP_API_ENDPOINT)
  @Macro
  @Description("The SOAP Endpoint URL associated for the Server-to-Server API integration. " +
          "For example, https://instance.soap.marketingcloudapis.com/Service.asmx")
  private final String soapEndpoint;

  public MarketingConnectorConfig(String clientId, String clientSecret, String authEndpoint, String soapEndpoint) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.authEndpoint = authEndpoint;
    this.soapEndpoint = soapEndpoint;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getAuthEndpoint() {
    return authEndpoint;
  }

  public String getSoapEndpoint() {
    return soapEndpoint;
  }

  /**
   * validates all the fields which are mandatory for the connection.
   */
  public void validateCredentials(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }

    if (Util.isNullOrEmpty(clientId)) {
      collector.addFailure("Client ID must be specified.", null)
              .withConfigProperty(MarketingCloudConstants.PROPERTY_CLIENT_ID);
    }

    if (Util.isNullOrEmpty(clientSecret)) {
      collector.addFailure("Client Secret must be specified.", null)
              .withConfigProperty(MarketingCloudConstants.PROPERTY_CLIENT_SECRET);
    }

    if (Util.isNullOrEmpty(authEndpoint)) {
      collector.addFailure("Auth Endpoint  must be specified.", null)
              .withConfigProperty(MarketingCloudConstants.PROPERTY_AUTH_API_ENDPOINT);
    }

    if (Util.isNullOrEmpty(soapEndpoint)) {
      collector.addFailure("Soap Endpoint must be specified.", null)
              .withConfigProperty(MarketingCloudConstants.PROPERTY_SOAP_API_ENDPOINT);
    }

    collector.getOrThrowException();
    validateSalesforceConnection(collector);
  }

  public void validateSalesforceConnection(FailureCollector collector) {

    try {
      MarketingCloudClient.create(getClientId(), getClientSecret(),
              getAuthEndpoint(),
              getSoapEndpoint());
    } catch (ETSdkException e) {
      collector.addFailure("Unable to connect to Salesforce Instance.",
                      "Ensure properties like Client ID, Client Secret, API Endpoint " +
                              ", Soap Endpoint, Auth Endpoint are correct.")
              .withConfigProperty(MarketingCloudConstants.PROPERTY_CLIENT_ID)
              .withConfigProperty(MarketingCloudConstants.PROPERTY_CLIENT_SECRET)
              .withConfigProperty(MarketingCloudConstants.PROPERTY_AUTH_API_ENDPOINT)
              .withConfigProperty(MarketingCloudConstants.PROPERTY_SOAP_API_ENDPOINT)
              .withStacktrace(e.getStackTrace());
    }
  }

  /**
   * Returns true if Salesforce can be connected to.
   */
  public boolean shouldConnect() {
    return !containsMacro(MarketingCloudConstants.PROPERTY_CLIENT_ID) &&
            !containsMacro(MarketingCloudConstants.PROPERTY_CLIENT_SECRET) &&
            !containsMacro(MarketingCloudConstants.PROPERTY_SOAP_API_ENDPOINT) &&
            !containsMacro(MarketingCloudConstants.PROPERTY_AUTH_API_ENDPOINT);
  }

  public Schema getSchema(SourceObject sourceObject) {
    try {
      MarketingCloudClient client = MarketingCloudClient.create(this.getClientId(),
              this.getClientSecret(),
              this.getAuthEndpoint(),
              this.getSoapEndpoint());
      return MarketingCloudInputFormat.getTableMetaData(sourceObject,
              "Stores",  client).getSchema();
    } catch (ETSdkException e) {
      throw new RuntimeException(e);
    }
  }
  private static final Logger LOG = LoggerFactory.getLogger(MarketingConnectorConfig.class);
  private Object getFieldValue(ETApiObject row, String fieldName) {
    try {
      Method method = row.getClass().getMethod(createGetterName(fieldName));
      return method.invoke(row);
    } catch (Exception e) {
      LOG.error(String.format("Error while fetching %s.%s value", row.getClass().getSimpleName(), fieldName), e);
      return null;
    }
  }
  private String createGetterName(String name) {
    StringBuilder sb = new StringBuilder("get");
    sb.append(name.substring(0, 1).toUpperCase());
    sb.append(name.substring(1));
    return sb.toString();
  }
  @VisibleForTesting
  String convertToStringValue(Object fieldValue) {
    return String.valueOf(fieldValue);
  }

  @VisibleForTesting
  Double convertToDoubleValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Double.parseDouble(String.valueOf(fieldValue));
  }

  @VisibleForTesting
  Integer convertToIntegerValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Integer.parseInt(String.valueOf(fieldValue));
  }

  @VisibleForTesting
  Boolean convertToBooleanValue(Object fieldValue) {
    if (fieldValue instanceof String && Strings.isNullOrEmpty(String.valueOf(fieldValue))) {
      return null;
    }

    return Boolean.parseBoolean(String.valueOf(fieldValue));
  }
  @VisibleForTesting
  Object convertToValue(String fieldName, Schema fieldSchema, Object fieldValue) {
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      return transformLogicalType(fieldName, logicalType, fieldValue);
    }

    switch (fieldType) {
      case STRING:
        return convertToStringValue(fieldValue);
      case DOUBLE:
        return convertToDoubleValue(fieldValue);
      case INT:
        return convertToIntegerValue(fieldValue);
      case BOOLEAN:
        return convertToBooleanValue(fieldValue);
      case UNION:
        if (fieldSchema.isNullable()) {
          return convertToValue(fieldName, fieldSchema.getNonNullable(), fieldValue);
        }
        throw new IllegalStateException(
                String.format("Field '%s' is of unexpected type '%s'. Declared 'complex UNION' types: %s",
                        fieldName, fieldValue.getClass().getSimpleName(), fieldSchema.getUnionSchemas()));
      default:
        throw new IllegalStateException(
                String.format("Record type '%s' is not supported for field '%s'", fieldType.name(), fieldName));
    }
  }
  public void convertRecord(SourceObject sourceObject, StructuredRecord.Builder recordBuilder, ETApiObject row)
          throws ETSdkException {
    MarketingCloudClient client =  MarketingCloudClient.create(getClientId(), getClientSecret(), getAuthEndpoint(),
            getSoapEndpoint());
    MarketingCloudObjectInfo sfObjectMetaData = client.fetchObjectSchema(sourceObject);
    List<Schema.Field> tableFields = sfObjectMetaData.getSchema().getFields();
    for (Schema.Field field : tableFields) {
      String fieldName = field.getName();
      Object rawFieldValue = null;
      if (row instanceof ETDataExtensionRow) {
        String apiFieldName = sfObjectMetaData.lookupFieldsMap(fieldName);
        rawFieldValue = ((ETDataExtensionRow) row).getColumn(apiFieldName);
      } else {
        rawFieldValue = getFieldValue(row, fieldName);
      }
      Object fieldValue = convertToValue(fieldName, field.getSchema(), rawFieldValue);

      recordBuilder.set(fieldName, fieldValue);
    }
  }
  private Object transformLogicalType(String fieldName, Schema.LogicalType logicalType, Object value) {
    switch (logicalType) {
      case TIMESTAMP_MICROS:
        if (value instanceof Date) {
          return TimeUnit.MILLISECONDS.toMicros((((Date) value).getTime()));
        }
        return null;
      default:
        throw new IllegalArgumentException(
                String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()));
    }
  }
}
