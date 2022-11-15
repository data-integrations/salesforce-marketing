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
Feature: Salesforce Marketing Cloud Sink - Design time scenarios

  @BATCH-TS-SFMC-DSGN-01
  Scenario Outline: Verify user should be able to successfully validate the sink for All Operation Types
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Select radio button plugin property: "operation" with value: "<OperationType>"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.clientsecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Select radio button plugin property: "truncateText" with value: "<TruncateText>"
    And Select radio button plugin property: "failOnError" with value: "<FailOnError>"
    And Select radio button plugin property: "replaceWithSpaces" with value: "<ReplaceWithSpaces>"
    And Replace input plugin property: "maxBatchSize" with value: "1000"
    Then Validate "Salesforce Marketing" plugin properties
    Examples:
      | OperationType | TruncateText | FailOnError | ReplaceWithSpaces |
      | INSERT        | TRUE         | FALSE       | TRUE              |
      | UPDATE        | FALSE        | TRUE        | TRUE              |
      | UPSERT        | FALSE        | FALSE       | TRUE              |
