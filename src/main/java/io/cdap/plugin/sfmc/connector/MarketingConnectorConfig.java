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

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.sfmc.source.MarketingCloudClient;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.Util;

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
}
