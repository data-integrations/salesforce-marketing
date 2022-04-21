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

import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.sfmcsource.locators.SfmcSourcePropertiesPage;
import io.cdap.plugin.utils.enums.DataRetrievalMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents - Salesforce Marketing Cloud - Source plugin - Properties page - Actions.
 */
public class SfmcSourcePropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(SfmcSourcePropertiesPageActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(SfmcSourcePropertiesPage.class);
  }

  public static void fillReferenceName(String referenceName) {
    logger.info("Fill Reference name: " + referenceName);
    SfmcSourcePropertiesPage.referenceNameInput.sendKeys(referenceName);
  }

  public static void selectDataRetrievalMode(DataRetrievalMode mode) {
    logger.info("Select Data Retrieval Mode: " + mode.value);
    SfmcSourcePropertiesPage.dataRetrievalModeDropdown.click();
    SfmcSourcePropertiesPage.getDropdownOptionElement(mode.value).click();
  }

  
}
