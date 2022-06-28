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
Feature: Salesforce Marketing Cloud Sink - Run time scenarios

  @SINK-TS-SFMC-RNTM-01 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to preview and run pipeline for Sink plugin
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
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Select radio button plugin property: "truncateText" with value: "true"
    And Select radio button plugin property: "failOnError" with value: "true"
    And Select radio button plugin property: "replaceWithSpaces" with value: "false"
    And Replace input plugin property: "maxBatchSize" with value: "1000"
    And Click on the Validate button
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |

  @SINK-TS-SFMC-RNTM-02 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to deploy and run pipeline for Sink plugin
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
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Select radio button plugin property: "truncateText" with value: "true"
    And Select radio button plugin property: "failOnError" with value: "true"
    And Select radio button plugin property: "replaceWithSpaces" with value: "false"
    And Replace input plugin property: "maxBatchSize" with value: "1000"
    And Click on the Validate button
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |
