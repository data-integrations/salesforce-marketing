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
Feature: Salesforce Marketing Cloud Source - Design time scenarios

  @TS-SFMC-DSGN-06
  Scenario: Verify user should be able to get Output Schema for Single Object Data Retrieval mode
    When I Open CDF Application
    And select data pipeline type as Data Pipeline - Batch
    And select plugin: "Salesforce Marketing" as data pipeline source
    And navigate to the properties page of plugin: "Salesforce Marketing"
