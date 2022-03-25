# Copyright © 2022 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@SFMarketingCloud
@SFMCSink
@Smoke
@Regression
Feature: Salesforce Marketing Cloud Sink - Design time scenarios (macros)

  @BATCH-TS-SFMC-DSGN-MACRO-01
  Scenario:Verify user should be able to validate the plugin when Authentication properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    Then Validate "Salesforce Marketing" plugin properties

  @BATCH-TS-SFMC-DSGN-MACRO-02
  Scenario:Verify user should be able to validate the plugin when Basic properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "dataExtension" and set the value to: "dataExtension"
    And Click on the Macro button of Property: "operation" and set the value to: "operation"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "authEndpoint" with value: "admin.authenticationbase.uri"
    And Enter input plugin property: "soapEndpoint" with value: "admin.soapapi.endpoint"
    Then Validate "Salesforce Marketing" plugin properties

  @BATCH-TS-SFMC-DSGN-MACRO-03
  Scenario:Verify user should be able to validate the plugin when Advanced properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Select radio button plugin property: "operation" with value: "UPDATE"
    And Enter input plugin property: "clientId" with value: "admin.clientid"
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret"
    And Enter input plugin property: "authEndpoint" with value: "admin.authenticationbase.uri"
    And Enter input plugin property: "soapEndpoint" with value: "admin.soapapi.endpoint"
    And Click on the Macro button of Property: "truncateText" and set the value to: "truncateText"
    And Click on the Macro button of Property: "failOnError" and set the value to: "failOnError"
    And Click on the Macro button of Property: "replaceWithSpaces" and set the value to: "replaceWithSpaces"
    And Click on the Macro button of Property: "maxBatchSize" and set the value to: "maxBatchSize"
    And Click on the Macro button of Property: "columnMapping" and set the value to: "columnMapping"
    Then Validate "Salesforce Marketing" plugin properties
