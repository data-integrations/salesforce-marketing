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

  @SINK-TS-SFMC-RNTM-01 @BQ_SOURCE_TABLE @BQ_SOURCE_CLEANUP
  Scenario:Verify user should be able to preview and deploy the pipeline when plugin is configured for Insert Operation
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "project.id" for Credentials and Authorization related fields
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId" for Credentials and Authorization related fields
    And Enter input plugin property: "dataset" with value: "dataset" for Credentials and Authorization related fields
    And Enter input plugin property: "table" with value: "bqsource.table"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Connect source as "BigQuery" and sink as "SalesforceDataExtension" to establish connection
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Stores"
    And Select radio button plugin property: "operation" with value: "INSERT"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Select radio button plugin property: "truncateText" with value: "true"
    And Select radio button plugin property: "failOnError" with value: "true"
    And Select radio button plugin property: "replaceWithSpaces" with value: "false"
    And Replace input plugin property: "maxBatchSize" with value: "1000"
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    And Wait till pipeline preview is in running state
    And Open and capture pipeline preview logs
    And Verify the preview run status of pipeline in the logs is "succeeded"
    And Close the pipeline logs
    And Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate record created in Sink application for Object is equal to expected record

  @SINK-TS-SFMC-RNTM-02 @BQ_SOURCE_UPDATE @BQ_SOURCE_CLEANUP
  Scenario:Verify user should be able to preview and deploy the pipeline when plugin is configured for Update Operation
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "project.id" for Credentials and Authorization related fields
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId" for Credentials and Authorization related fields
    And Enter input plugin property: "dataset" with value: "dataset" for Credentials and Authorization related fields
    And Enter input plugin property: "table" with value: "bqsource.table"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Connect source as "BigQuery" and sink as "SalesforceDataExtension" to establish connection
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Stores"
    And Select radio button plugin property: "operation" with value: "UPDATE"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Select radio button plugin property: "truncateText" with value: "true"
    And Select radio button plugin property: "failOnError" with value: "true"
    And Select radio button plugin property: "replaceWithSpaces" with value: "false"
    And Replace input plugin property: "maxBatchSize" with value: "1000"
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    And Wait till pipeline preview is in running state
    And Open and capture pipeline preview logs
    And Verify the preview run status of pipeline in the logs is "succeeded"
    And Close the pipeline logs
    And Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate record created in Sink application for Object is equal to expected record
