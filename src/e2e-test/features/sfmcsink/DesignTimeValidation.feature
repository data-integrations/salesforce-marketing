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
Feature: Salesforce Marketing Cloud Sink - Design time validation scenarios

  @BATCH-TS-SFMC-DSGN-ERROR-01
  Scenario: Verify required fields missing validation for properties
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | referenceName |
      | clientId      |
      | clientSecret  |
      | authEndpoint  |
      | soapEndpoint  |

  @BATCH-TS-SFMC-DSGN-ERROR-02 @BQ_SOURCE_TABLE @BQ_SOURCE_CLEANUP
  Scenario: Verify validation message when user provides invalid Authentication Properties
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
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Enter input plugin property: "clientId" with value: "invalid.clientid"
    And Enter input plugin property: "clientSecret" with value: "invalid.clientsecret"
    And Enter input plugin property: "authEndpoint" with value: "invalid.authenticationbase.uri"
    And Enter input plugin property: "soapEndpoint" with value: "invalid.soapapi.endpoint"
    And Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "required.property.invalidcredentials" on the header

  @BATCH-TS-SFMC-DSGN-ERROR-03
  Scenario: Verify required fields missing validation for Data Extension External Key property
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | dataExtension |
