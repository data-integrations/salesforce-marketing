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
@SFMCSource
@Smoke
@Regression

Feature: Salesforce Marketing Cloud Source - Design time Scenarios (macros)

  @BATCH-TS-SFMC-DSGN-MACRO-01
  Scenario:Verify user should be able to validate the plugin when Authentication properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "objectName" and set the value to: "objectName"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    And Click on the Macro button of Property: "restEndpoint" and set the value to: "restEndpoint"
    Then Validate "Salesforce Marketing" plugin properties

  @BATCH-TS-SFMC-DSGN-MACRO-02
  Scenario:Verify user should be able to validate the plugin when Single Object Retrieval properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Click on the Macro button of Property: "queryMode" and set the value to: "queryMode"
    And Click on the Macro button of Property: "objectName" and set the value to: "objectName"
    And Click on the Macro button of Property: "dataExtensionKey" and set the value to: "dataExtensionKey"
    Then Validate "Salesforce Marketing" plugin properties

  @BATCH-TS-SFMC-DSGN-MACRO-03
  Scenario:Verify user should be able to validate the plugin when Multi Object Retrieval properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Click on the Macro button of Property: "queryMode" and set the value to: "queryMode"
    And Click on the Macro button of Property: "objectList" and set the value to: "objectList"
    And Click on the Macro button of Property: "dataExtensionKeyList" and set the value to: "dataExtensionKeyList"
    And Click on the Macro button of Property: "tableNameField" and set the value to: "tablename"
    Then Validate "Salesforce Marketing" plugin properties

  @BATCH-TS-SFMC-DSGN-MACRO-04
  Scenario:Verify user should be able to validate the plugin when configured for Filter Property with macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Click on the Macro button of Property: "objectName" and set the value to: "objectName"
    And Click on the Macro button of Property: "filter" and set the value to: "filter"
    Then Validate "Salesforce Marketing" plugin properties
