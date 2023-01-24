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

package io.cdap.plugin.sfmcsink.actions;

import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.sfmc.source.MarketingCloudClient;
import io.cdap.plugin.sfmcsource.locators.SfmcSourcePropertiesPage;
import io.cdap.plugin.tests.hooks.TestSetupHooks;
import org.junit.Assert;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Represents - Salesforce Marketing Cloud - Sink plugin - Properties page - Actions.
 */
public class SfmcSinkPropertiesPageActions {
  private static Gson gson = new Gson();
  private static String projectId = System.getenv("PROJECT_ID");
  private static String dataset = System.getenv("SALESFORCE_MARKETING_DATASET");

  static {
    SeleniumHelper.getPropertiesLocators(SfmcSourcePropertiesPage.class);
  }

  private static TableResult getDataExtensionUniqueIdFromBigQuery(String dataset, String table)
    throws IOException, InterruptedException {
    String id = TestSetupHooks.storeid;
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t WHERE " +
      "storeid='" + id + "' ";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);

    return result;
  }

  public static void verifyIfRecordsCreatedInSfmcSinkAreCorrect() throws ETSdkException,
    IOException, InterruptedException {
    ETResponse<ETDataExtensionRow> dataExtensionResponse;
    String filter = "storeid=" + TestSetupHooks.storeid;

    MarketingCloudClient client = MarketingCloudClient.create(
      System.getenv("SALESFORCE_MARKETING_CLIENT_ID"),
      System.getenv("SALESFORCE_MARKETING_CLIENT_SECRET"),
      System.getenv("SALESFORCE_MARKETING_BASE_URI"),
      System.getenv("SALESFORCE_MARKETING_SOAP_API_ENDPOINT"));

    dataExtensionResponse = client.fetchDataExtensionRecords("Stores", filter, null);
    TableResult bigQueryTableData = getDataExtensionUniqueIdFromBigQuery(
      dataset, PluginPropertyUtils.pluginProp("bqsource.table"));
    String bigQueryJsonResponse = bigQueryTableData.getValues().iterator().next().get(0).getValue().toString();
    JsonObject jsonObject = gson.fromJson(bigQueryJsonResponse, JsonObject.class);
    Map<String, Object> bigQueryResponseInMap = gson.fromJson(jsonObject.toString(), Map.class);
    Set<String> bigQueryKeySet = bigQueryResponseInMap.keySet();

    for (String key : bigQueryKeySet) {
      Assert.assertEquals(bigQueryResponseInMap.get(key), dataExtensionResponse.getResult().getObject().getColumn(key));
    }
  }
}
