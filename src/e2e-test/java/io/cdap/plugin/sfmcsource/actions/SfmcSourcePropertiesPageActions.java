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

package io.cdap.plugin.sfmcsource.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.sfmcsource.locators.SfmcSourcePropertiesPage;
import io.cdap.plugin.utils.enums.Sobjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Represents - Salesforce Marketing Cloud - Source plugin - Properties page - Actions.
 */
public class SfmcSourcePropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(SfmcSourcePropertiesPageActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(SfmcSourcePropertiesPage.class);
  }

  public static void configureSourcePluginForObjectNameInSingleObjectMode(Sobjects objectName) {
    logger.info("Select dropdown option: " + objectName.value);
    ElementHelper.selectDropdownOption(SfmcSourcePropertiesPage.objectDropdownForSIngleObjectMode,
      CdfPluginPropertiesLocators.locateDropdownListItem(objectName.value));
  }

  public static void selectObjectNamesInMultiObjectMode(List<Sobjects> objectNames) {
    int totalSObjects = objectNames.size();

    SfmcSourcePropertiesPage.objectDropdownForMultiObjectMode.click();

    for (int i = 0; i < totalSObjects; i++) {
      logger.info("Select checkbox option: " + objectNames.get(i).value);
      ElementHelper.selectCheckbox(SfmcSourcePropertiesPage.
        locateObjectCheckBoxInMultiObjectsSelector(objectNames.get(i).value));
    }

    //We need to click on the Plugin Properties page header to dismiss the dialog
    ElementHelper.clickUsingActions(CdfPluginPropertiesLocators.pluginPropertiesPageHeader);
  }

  public static void fillDataExtensionExternalKey(String key) {
    ElementHelper.sendKeys(SfmcSourcePropertiesPage.dataExtensionExternalKeyInputForMultiObjectMode, key);
  }
}
