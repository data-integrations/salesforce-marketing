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
Feature: Salesforce Marketing Cloud Sink - Run time scenarios (macros)

  @SINK-TS-SFMC-RNTM-MACRO-01 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to preview and run pipeline using macro for Sink
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce Marketing" to establish connection
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "dataExtension" and set the value to: "dataExtension"
    And Select radio button plugin property: "operation" with value: "<OperationType>"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    And Select radio button plugin property: "truncateText" with value: "true"
    And Select radio button plugin property: "failOnError" with value: "true"
    And Select radio button plugin property: "replaceWithSpaces" with value: "false"
    And Replace input plugin property: "maxBatchSize" with value: "1000"
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "sink.dataextensionkey" for key "dataExtension"
    And Enter runtime argument value from environment variable "admin.clientid" for key "clientId"
    And Enter runtime argument value from environment variable "admin.cliensecret" for key "clientSecret"
    And Enter runtime argument value from environment variable "admin.base.uri" for key "authEndpoint"
    And Enter runtime argument value from environment variable "admin.soap.endpoint" for key "soapEndpoint"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |

  @SINK-TS-SFMC-RNTM-MACRO-02 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to deploy and run pipeline using macro for Sink
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce Marketing" to establish connection
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "dataExtension" and set the value to: "dataExtension"
    And Select radio button plugin property: "operation" with value: "<OperationType>"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "authEndpoint" and set the value to: "authEndpoint"
    And Click on the Macro button of Property: "soapEndpoint" and set the value to: "soapEndpoint"
    And Select radio button plugin property: "truncateText" with value: "true"
    And Select radio button plugin property: "failOnError" with value: "true"
    And Select radio button plugin property: "replaceWithSpaces" with value: "false"
    And Replace input plugin property: "maxBatchSize" with value: "1000"
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "sink.dataextensionkey" for key "dataExtension"
    And Enter runtime argument value from environment variable "admin.clientid" for key "clientId"
    And Enter runtime argument value from environment variable "admin.cliensecret" for key "clientSecret"
    And Enter runtime argument value from environment variable "admin.base.uri" for key "authEndpoint"
    And Enter runtime argument value from environment variable "admin.soap.endpoint" for key "soapEndpoint"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |

  @SINK-TS-SFMC-RNTM-MACRO-03 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to preview and run pipeline when plugin is configured for Advanced section using macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce Marketing" to establish connection
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Select radio button plugin property: "operation" with value: "<OperationType>"
    And Enter input plugin property: "clientId" with value: "SALESFORCE_MARKETING_CLIENT_ID" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "SALESFORCE_MARKETING_CLIENT_SECRET" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "SALESFORCE_MARKETING_BASEURI" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "SALESFORCE_MARKETING_SOAP_API_ENDPOINT" for Credentials and Authorization related fields
    And Click on the Macro button of Property: "truncateText" and set the value to: "truncateText"
    And Click on the Macro button of Property: "failOnError" and set the value to: "failOnError"
    And Click on the Macro button of Property: "replaceWithSpaces" and set the value to: "replaceWithSpaces"
    And Click on the Macro button of Property: "maxBatchSize" and set the value to: "maxBatchSize"
    And Click on the Macro button of Property: "columnMapping" and set the value to: "columnMapping"
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "TRUE" for key "truncateText"
    And Enter runtime argument value "TRUE" for key "failOnError"
    And Enter runtime argument value "TRUE" for key "replaceWithSpaces"
    And Enter runtime argument value "sink.maxbatchsize" for key "maxBatchSize"
    And Enter runtime argument value "sink.columnmapping" for key "columnMapping"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |

  @SINK-TS-SFMC-RNTM-MACRO-04 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to deploy and run pipeline when plugin is configured for Advanced section using macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce Marketing" to establish connection
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Select radio button plugin property: "operation" with value: "<OperationType>"
    And Enter input plugin property: "clientId" with value: "SALESFORCE_MARKETING_CLIENT_ID" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "SALESFORCE_MARKETING_CLIENT_SECRET" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "SALESFORCE_MARKETING_BASEURI" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "SALESFORCE_MARKETING_SOAP_API_ENDPOINT" for Credentials and Authorization related fields
    And Click on the Macro button of Property: "truncateText" and set the value to: "truncateText"
    And Click on the Macro button of Property: "failOnError" and set the value to: "failOnError"
    And Click on the Macro button of Property: "replaceWithSpaces" and set the value to: "replaceWithSpaces"
    And Click on the Macro button of Property: "maxBatchSize" and set the value to: "maxBatchSize"
    And Click on the Macro button of Property: "columnMapping" and set the value to: "columnMapping"
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "TRUE" for key "truncateText"
    And Enter runtime argument value "TRUE" for key "failOnError"
    And Enter runtime argument value "TRUE" for key "replaceWithSpaces"
    And Enter runtime argument value "sink.maxbatchsize" for key "maxBatchSize"
    And Enter runtime argument value "sink.columnmapping" for key "columnMapping"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |
