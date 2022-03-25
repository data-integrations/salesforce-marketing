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

import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.sfmcsource.locators.SfmcSourcePropertiesPage;
import io.cdap.plugin.utils.enums.Sobjects;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents - Salesforce Marketing Cloud - Source plugin - Properties page - Actions.
 */
public class SfmcSourcePropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(SfmcSourcePropertiesPageActions.class);
  private static Gson gson = new Gson();
  private static String projectId = System.getenv("PROJECT_ID");
  private static String dataset = System.getenv("SALESFORCE_MARKETING_DATASET");

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
    SfmcSourcePropertiesPage.selectOptionDataExtension.click();
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

  public static void verifyIfRecordCreatedInSinkForObjectIsCorrect(String expectedOutputFile)
    throws IOException, InterruptedException {
    List<String> expectedOutput = new ArrayList<>();
    List<String> bigQueryRows = new ArrayList<>();
    try (BufferedReader bf1 = Files.newBufferedReader(Paths.get(PluginPropertyUtils.pluginProp(expectedOutputFile)))) {
      String line;
      while ((line = bf1.readLine()) != null) {
        expectedOutput.add(line);
      }
    }

    for (int expectedRow = 0; expectedRow < expectedOutput.size(); expectedRow++) {
      JsonObject expectedOutputAsJson = gson.fromJson(expectedOutput.get(expectedRow), JsonObject.class);
      String uniqueId = expectedOutputAsJson.get("id").getAsString();
      getBigQueryTableData(dataset, PluginPropertyUtils.pluginProp("bqtarget.table"), uniqueId, bigQueryRows);

    }
    for (int row = 0; row < bigQueryRows.size() && row < expectedOutput.size(); row++) {
      Assert.assertTrue(compareValueOfBothResponses(expectedOutput.get(row), bigQueryRows.get(row)));
    }
  }

  private static boolean compareValueOfBothResponses(String sfmcResponse, String bigQueryResponse) {
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    Map<String, Object> sfmcResponseInmap = gson.fromJson(sfmcResponse, type);
    Map<String, Object> bigQueryResponseInMap = gson.fromJson(bigQueryResponse, type);
    MapDifference<String, Object> mapDifference = Maps.difference(sfmcResponseInmap, bigQueryResponseInMap);
    logger.info("Assertion :" + mapDifference);

    return mapDifference.areEqual();
  }

  private static void getBigQueryTableData(String dataset, String table, String uniqueId,
                                                   List<String> bigQueryRows)
    throws IOException, InterruptedException {
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t WHERE " +
      "id='" + uniqueId + "' ";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue().toString()));
  }

  private static TableResult getTableNamesFromDataSet(String bqTargetDataset) throws IOException, InterruptedException {
    String selectQuery = "SELECT table_name FROM `" + projectId + "." + bqTargetDataset +
      "`.INFORMATION_SCHEMA.TABLES ";

    return BigQueryClient.getQueryResult(selectQuery);
  }

  public static void verifyIfRecordCreatedInSinkForMultipleObjectsAreCorrect(String expectedOutputFile)
    throws IOException, InterruptedException {
    List<String> expectedOutput = new ArrayList<>();
    List<String> bigQueryRows = new ArrayList<>();
    try (BufferedReader bf1 = Files.newBufferedReader(Paths.get(PluginPropertyUtils.pluginProp(expectedOutputFile)))) {
      String line;
      while ((line = bf1.readLine()) != null) {
        expectedOutput.add(line);
      }
    }

    List<String> bigQueryDatasetTables = new ArrayList<>();
    TableResult tablesSchema = getTableNamesFromDataSet(dataset);
    tablesSchema.iterateAll().forEach(value -> bigQueryDatasetTables.add(value.get(0).getValue().toString()));

    for (int expectedRow = 0; expectedRow < expectedOutput.size(); expectedRow++) {
      JsonObject expectedOutputAsJson = gson.fromJson(expectedOutput.get(expectedRow), JsonObject.class);
      String uniqueId = expectedOutputAsJson.get("id").getAsString();
      getBigQueryTableData(dataset, bigQueryDatasetTables.get(0), uniqueId, bigQueryRows);
    }
    for (int row = 0; row < bigQueryRows.size() && row < expectedOutput.size(); row++) {
      Assert.assertTrue(compareValueOfBothResponses(expectedOutput.get(row), bigQueryRows.get(row)));
    }
  }
}
