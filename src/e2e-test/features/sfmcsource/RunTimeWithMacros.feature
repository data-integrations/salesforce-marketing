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

Feature: Salesforce Marketing Cloud Source - Run time Scenarios (macros)

  @BATCH-TS-SFMC-RNTM-MACRO-01 @BQ_SINK_TEST
  Scenario : Verify user should be able to preview the pipeline when plugin is configured for Single Object Data Retrieval mode with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Select dropdown plugin property: "select-queryMode" with option value: "Single Object"
    And Click on the Macro button of Property: "objectName" and set the value to: "objectName"
    And Click on the Macro button of Property: "dataExtensionKey" and set the value to: "dataExtensionKey"
    And Click on the Macro button of Property: "filter" and set the value to: "filter"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    And Click on the Macro button of Property: "restEndpoint" and set the value to: "restEndpoint"
    And Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "singleobjectmode.objectname" for key "objectName"
    And Enter runtime argument value "singleobjectmode.dataextensionkey" for key "dataExtensionKey"
    And Enter runtime argument value "filter.value" for key "filter"
    And Enter runtime argument value from environment variable "admin.clientid" for key "clientId"
    And Enter runtime argument value from environment variable "admin.cliensecret" for key "clientSecret"
    And Enter runtime argument value from environment variable "admin.base.uri" for key "authEndpoint"
    And Enter runtime argument value from environment variable "admin.soap.endpoint" for key "soapEndpoint"
    And Enter runtime argument value from environment variable "admin.rest.endpoint" for key "restEndpoint"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-SFMC-RNTM-MACRO-02 @BQ_SINK_TEST
  Scenario : Verify user should be able to deploy and run the pipeline when plugin is configured for Single Object Data Retrieval mode with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Select dropdown plugin property: "select-queryMode" with option value: "Single Object"
    And Click on the Macro button of Property: "objectName" and set the value to: "objectName"
    And Click on the Macro button of Property: "dataExtensionKey" and set the value to: "dataExtensionKey"
    And Click on the Macro button of Property: "filter" and set the value to: "filter"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    And Click on the Macro button of Property: "restEndpoint" and set the value to: "restEndpoint"
    And Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "singleobjectmode.objectname" for key "objectName"
    And Enter runtime argument value "singleobjectmode.dataextensionkey" for key "dataExtensionKey"
    And Enter runtime argument value "filter.value" for key "filter"
    And Enter runtime argument value from environment variable "admin.clientid" for key "clientId"
    And Enter runtime argument value from environment variable "admin.cliensecret" for key "clientSecret"
    And Enter runtime argument value from environment variable "admin.base.uri" for key "authEndpoint"
    And Enter runtime argument value from environment variable "admin.soap.endpoint" for key "soapEndpoint"
    And Enter runtime argument value from environment variable "admin.rest.endpoint" for key "restEndpoint"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

  @BATCH-TS-SFMC-RNTM-MACRO-03 @BQ_SINK_TEST
  Scenario : Verify user should be able to preview the pipeline when plugin is configured for Multi Object Data Retrieval mode with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Select dropdown plugin property: "select-queryMode" with option value: "Multi Object"
    And Click on the Macro button of Property: "objectList" and set the value to: "objectList"
    And Click on the Macro button of Property: "dataExtensionKeyList" and set the value to: "dataExtensionKeyList"
    And Click on the Macro button of Property: "tableNameField" and set the value to: "tablename"
    And Click on the Macro button of Property: "filter" and set the value to: "filter"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    And Click on the Macro button of Property: "restEndpoint" and set the value to: "restEndpoint"
    And Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "multiobjectmode.objectlist" for key "objectList"
    And Enter runtime argument value "multiobjectmode.dataextensionkeylist" for key "dataExtensionKey"
    And Enter runtime argument value "multiobjectmode.tablename" for key "tablename"
    And Enter runtime argument value "filter.value" for key "filter"
    And Enter runtime argument value from environment variable "admin.clientid" for key "clientId"
    And Enter runtime argument value from environment variable "admin.cliensecret" for key "clientSecret"
    And Enter runtime argument value from environment variable "admin.base.uri" for key "authEndpoint"
    And Enter runtime argument value from environment variable "admin.soap.endpoint" for key "soapEndpoint"
    And Enter runtime argument value from environment variable "admin.rest.endpoint" for key "restEndpoint"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-SFMC-RNTM-MACRO-04 @BQ_SINK_TEST
  Scenario : Verify user should be able to deploy and run the pipeline when plugin is configured for Multi Object Data Retrieval mode with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Select dropdown plugin property: "select-queryMode" with option value: "Multi Object"
    And Click on the Macro button of Property: "objectList" and set the value to: "objectList"
    And Click on the Macro button of Property: "dataExtensionKeyList" and set the value to: "dataExtensionKeyList"
    And Click on the Macro button of Property: "tableNameField" and set the value to: "tablename"
    And Click on the Macro button of Property: "filter" and set the value to: "filter"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    And Click on the Macro button of Property: "restEndpoint" and set the value to: "restEndpoint"
    And Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "multiobjectmode.objectlist" for key "objectList"
    And Enter runtime argument value "multiobjectmode.dataextensionkeylist" for key "dataExtensionKey"
    And Enter runtime argument value "multiobjectmode.tablename" for key "tablename"
    And Enter runtime argument value "filter.value" for key "filter"
    And Enter runtime argument value from environment variable "admin.clientid" for key "clientId"
    And Enter runtime argument value from environment variable "admin.cliensecret" for key "clientSecret"
    And Enter runtime argument value from environment variable "admin.base.uri" for key "authEndpoint"
    And Enter runtime argument value from environment variable "admin.soap.endpoint" for key "soapEndpoint"
    And Enter runtime argument value from environment variable "admin.rest.endpoint" for key "restEndpoint"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
