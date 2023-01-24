/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sfmcsink.stepsdesign;

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.sfmcsink.actions.SfmcSinkPropertiesPageActions;
import io.cucumber.java.en.Then;

import java.io.IOException;

/**
 * Represents Salesforce Marketing Cloud - Sink plugin - Properties page - steps.
 */
public class DesignTimeSteps implements CdfHelper {

  @Then("Validate record created in Sink application for Object is equal to expected record")
  public void validateRecordCreatedInSinkApplicationForObjectIsEqualToExpectedRecord() throws IOException,
    ETSdkException, InterruptedException {
    SfmcSinkPropertiesPageActions.verifyIfRecordsCreatedInSfmcSinkAreCorrect();
  }
}
