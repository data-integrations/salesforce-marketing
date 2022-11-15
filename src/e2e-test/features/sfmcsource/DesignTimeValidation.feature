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
Feature: Salesforce Marketing Cloud Source - Design time Validation scenarios

  @BATCH-TS-SFMC-DSGN-ERROR-01
  Scenario: Verify required fields missing validation for listed properties
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | referenceName |
      | clientId     |
      | clientSecret |
      | authEndpoint |
      | soapEndpoint |
      | restEndpoint |

  @BATCH-TS-SFMC-DSGN-ERROR-02
  Scenario: Verify validation message when user provides invalid Authentication Properties
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Marketing" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce Marketing"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "clientId" with value: "invalid.clientid"
    And Enter input plugin property: "clientSecret" with value: "invalid.clientsecret"
    And Enter input plugin property: "authEndpoint" with value: "invalid.authenticationbase.uri"
    And Enter input plugin property: "soapEndpoint" with value: "invalid.soapapi.endpoint"
    And Enter input plugin property: "restEndpoint" with value: "invalid.restapibase.uri"
    And Click on the Validate button
    Then Verify invalid credentials validation message for below listed properties:
      | clientId     |
      | clientSecret |
      | authEndpoint |
      | soapEndpoint |
      | restEndpoint |

  @BATCH-TS-SFMC-DSGN-ERROR-03
  Scenario:Verify required fields missing validation for Data Extension External key property when object is selected as Data Extension in Single Object mode
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
    And Select dropdown plugin property: "select-objectName" with option value: "Data Extension"
    And Click on the Validate button
    Then Verify that the Plugin Property: "dataExtensionKey" is displaying an in-line error message: "required.property.dataextensionkeysingleobject"

  @BATCH-TS-SFMC-DSGN-ERROR-04
  Scenario:Verify required fields missing validation for Data Extension External key property when object is selected as Data Extension in Multi Object mode
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
    And fill Object List with below listed Objects in the Multi Object mode:
      | DATA_EXTENSION | BOUNCE_EVENT |
    And Click on the Validate button
    Then Verify that the Plugin Property: "dataExtensionKeyList" is displaying an in-line error message: "required.property.dataextensionkeymultiobject"
