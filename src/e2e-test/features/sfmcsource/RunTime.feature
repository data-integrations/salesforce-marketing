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

  @BATCH-TS-SFMC-RNTM-01 @BQ_SINK_TEST
  Scenario Outline: Verify user should be able to preview the pipeline when plugin is configured for Object Name in Single Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Select dropdown plugin property: "select-queryMode" with option value: "Single Object"
    And configure source plugin for Object: "<ObjectName>" in the Single Object mode
    Then Validate "Salesforce Marketing" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "success"
    And Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin
    Examples:
      | ObjectName   |
      | BOUNCE_EVENT |

  @BATCH-TS-SFMC-RNTM-02 @BQ_SINK_TEST
  Scenario Outline: Verify user should be able to deploy and run the pipeline when plugin is configured for Object Name in Single Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Select dropdown plugin property: "select-queryMode" with option value: "Single Object"
    And configure source plugin for Object: "<ObjectName>" in the Single Object mode
    Then Validate "Salesforce Marketing" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    And Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin
    Examples:
      | ObjectName   |
      | BOUNCE_EVENT |

  @BATCH-TS-SFMC-RNTM-03 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured for Object List in Multi Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Select dropdown plugin property: "select-queryMode" with option value: "Multi Object"
    And fill Object List with below listed Objects in the Multi Object mode:
      | BOUNCE_EVENT | NOTSENT_EVENT |
    And Enter input plugin property: "tableNameField" with value: "tablename"
    Then Validate "Salesforce Marketing" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "success"

  @BATCH-TS-SFMC-RNTM-04 @BQ_SINK_TEST
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured for Object List in Multi Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Enter input plugin property: "restEndpoint" with value: "admin.rest.endpoint" for Credentials and Authorization related fields
    And Select dropdown plugin property: "select-queryMode" with option value: "Multi Object"
    And fill Object List with below listed Objects in the Multi Object mode:
      | BOUNCE_EVENT | NOTSENT_EVENT |
    And Enter input plugin property: "tableNameField" with value: "tablename"
    Then Validate "Salesforce Marketing" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"

  @BATCH-TS-SFMC-RNTM-05 @BQ_SINK_TEST
  Scenario Outline: Verify user should be able to preview the pipeline when plugin is configured for Filter property in Single Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
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
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "success"
    And Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin
    Examples:
      | ObjectName   | Filter       |
      | BOUNCE_EVENT | filter.value |

  @BATCH-TS-SFMC-RNTM-06 @BQ_SINK_TEST
  Scenario Outline: Verify user should be able to deploy and run the pipeline when plugin is configured for Filter property in Single Object Data Retrieval mode
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
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
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce Marketing" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    And Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin
    Examples:
      | ObjectName   | Filter       |
      | BOUNCE_EVENT | filter.value |
