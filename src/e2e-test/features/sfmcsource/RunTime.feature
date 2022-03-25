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
Feature: Salesforce Marketing Cloud Source - Run time Scenarios

  @BATCH-TS-SFMC-RNTM-01 @BQ_SINK @FILE_PATH @BQ_SINK_CLEANUP
  Scenario Outline: Verify user should be able to preview and deploy the pipeline when plugin is configured for Object Name in Single Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Select dropdown plugin property: "select-queryMode" with option value: "Single Object"
    And configure source plugin for Object: "<ObjectName>" in the Single Object mode
    Then Validate "Salesforce Marketing" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Connect source as "Salesforce-Marketing" and sink as "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "project.id" for Credentials and Authorization related fields
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId" for Credentials and Authorization related fields
    And Enter input plugin property: "dataset" with value: "dataset" for Credentials and Authorization related fields
    And Enter input plugin property: "table" with value: "bqtarget.table"
    Then Validate "BigQuery" plugin properties
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
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    And Close the pipeline logs
    Then Validate record created in Sink application for Object is equal to expected output file "expectedOutputFile"
    Examples:
      | ObjectName   |
      | MAILING_LIST |

  @BATCH-TS-SFMC-RNTM-02 @FILE_PATH @BQ_MULTI_CLEANUP
  Scenario: Verify user should be able to preview and deploy the pipeline when plugin is configured for Object List in Multi Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Select dropdown plugin property: "select-queryMode" with option value: "Multi Object"
    And Enter input plugin property: "filter" with value: "filter.value"
    And fill Object List with below listed Objects in the Multi Object mode:
      | MAILING_LIST |
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "project.id" for Credentials and Authorization related fields
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId" for Credentials and Authorization related fields
    And Enter input plugin property: "dataset" with value: "dataset" for Credentials and Authorization related fields
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce-Marketing" and sink as "BigQueryMultiTable" to establish connection
    And Preview and run the pipeline
    And Wait till pipeline preview is in running state
    And Open and capture pipeline preview logs
    And Verify the preview run status of pipeline in the logs is "succeeded"
    And Close the pipeline logs
    And Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    And Close the pipeline logs
    Then Validate record created in Sink application for Multi object mode is equal to expected output file "expectedOutputFile1"

  @BATCH-TS-SFMC-RNTM-03 @BQ_SINK @FILE_PATH @BQ_SINK_CLEANUP
  Scenario Outline: Verify user should be able to preview the pipeline when plugin is configured for Filter property in Single Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Select dropdown plugin property: "select-queryMode" with option value: "Single Object"
    And configure source plugin for Object: "<ObjectName>" in the Single Object mode
    And Enter input plugin property: "filter" with value: "<Filter>"
    Then Validate "Salesforce Marketing" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "project.id" for Credentials and Authorization related fields
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId" for Credentials and Authorization related fields
    And Enter input plugin property: "dataset" with value: "dataset" for Credentials and Authorization related fields
    And Enter input plugin property: "table" with value: "bqtarget.table"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce-Marketing" and sink as "BigQueryTable" to establish connection
    And Preview and run the pipeline
    And Wait till pipeline preview is in running state
    And Open and capture pipeline preview logs
    And Verify the preview run status of pipeline in the logs is "succeeded"
    And Close the pipeline logs
    And Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    And Close the pipeline logs
    Then Validate record created in Sink application for Object is equal to expected output file "expectedOutputFile1"
    Examples:
      | ObjectName   | Filter       |
      | MAILING_LIST | filter.value |
