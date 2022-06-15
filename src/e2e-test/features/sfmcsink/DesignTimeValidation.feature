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
    And Select data pipeline type as: "Batch"
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

  @BATCH-TS-SFMC-DSGN-ERROR-02
  Scenario: Verify validation message when user provides invalid Authentication Properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "dataExtension" with value: "Key121"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Click on the Validate button
    Then Verify invalid credentials validation message for below listed properties:
      | clientId     |
      | clientSecret |
      | authEndpoint |
      | soapEndpoint |

  @BATCH-TS-SFMC-DSGN-ERROR-03
  Scenario: Verify required fields missing validation for Data Extension External Key property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select Sink plugin: "SalesforceDataExtension" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "admin.clientid" for Credentials and Authorization related fields
    And Enter input plugin property: "clientSecret" with value: "admin.cliensecret" for Credentials and Authorization related fields
    And Enter input plugin property: "authEndpoint" with value: "admin.base.uri" for Credentials and Authorization related fields
    And Enter input plugin property: "soapEndpoint" with value: "admin.soap.endpoint" for Credentials and Authorization related fields
    And Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | dataExtension |
