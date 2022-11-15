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

package io.cdap.plugin.sfmcsource.stepsdesign;

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.sfmcsource.actions.SfmcSourcePropertiesPageActions;
import io.cdap.plugin.utils.enums.Sobjects;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents Salesforce Marketing Cloud - Source plugin - Properties page - steps.
 */
public class DesignTimeSteps implements CdfHelper {

  @And("configure source plugin for Object: {string} in the Single Object mode")
  public void configureSourcePluginForObjectInTheSingleObjectMode(String objectName) {
    SfmcSourcePropertiesPageActions.configureSourcePluginForObjectNameInSingleObjectMode(Sobjects.valueOf(objectName));
  }

  @And("fill Object List with below listed Objects in the Multi Object mode:")
  public void selectObjectsInObjectsList(DataTable table) {
    List<Sobjects> objectsList = new ArrayList<>();
    List<String> list = table.asList();

    for (String object : list) {
      objectsList.add(Sobjects.valueOf(object));
    }
    SfmcSourcePropertiesPageActions.selectObjectNamesInMultiObjectMode(objectsList);
  }

  @And("Enter input plugin property Data Extension external key as: {string}")
  public void enterInputPluginPropertyDataExtensionExternalKey(String key) {
    SfmcSourcePropertiesPageActions.fillDataExtensionExternalKey(key);
  }

  @Then("Validate record created in Sink application for Object is equal to expected output file {string}")
  public void validateRecordCreatedInSinkApplicationForViewTemplateIsEqualToExpectedOutputFile(String
  expectedOutputFile) throws IOException, InterruptedException {
    SfmcSourcePropertiesPageActions.verifyIfRecordCreatedInSinkForObjectIsCorrect(expectedOutputFile);
  }

  @Then("Validate record created in Sink application for Multi object mode is equal to expected output file {string}")
  public void validateRecordCreatedInSinkApplicationForMultiObjectModeIsEqualToExpectedOutputFile(String
  expectedOutputFile) throws IOException, InterruptedException {
    SfmcSourcePropertiesPageActions.verifyIfRecordCreatedInSinkForMultipleObjectsAreCorrect(expectedOutputFile);
  }
}
