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


package io.cdap.plugin.tests.hooks;

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Represents Test Setup and Clean up hooks.
 */

public class TestSetupHooks {
  private static boolean firstFileSinkTestFlag = true;
  public static String storeid;
  private static String projectId = System.getenv("PROJECT_ID");
  @Before(order = 1, value = "@BQ_SINK")
  public void setTempTargetBQDataset() {
    String bqTargetDataset = System.getenv("SALESFORCE_MARKETING_DATASET");
    BeforeActions.scenario.write("BigQuery Target dataset name: " + bqTargetDataset);
  }

  @Before(order = 2, value = "@BQ_SINK")
  public void setTempTargetBQTable() {
    String bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("bqtarget.table", bqTargetTable);
    BeforeActions.scenario.write("BigQuery Target table name: " + bqTargetTable);
  }

  @After(order = 1, value = "@BQ_SINK_CLEANUP")
  public void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTable = PluginPropertyUtils.pluginProp("bqtarget.table");
    try {
      dropBqTable(bqTargetTable);
      BeforeActions.scenario.write("BigQuery Target table: " + bqTargetTable + " is deleted successfully");
      bqTargetTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BigQuery Target Table: " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @After(order = 1, value = "@BQ_MULTI_CLEANUP")
  public void deleteMultiSourceTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTable = PluginPropertyUtils.pluginProp("multisource.table");
    try {
      dropBqTable(bqTargetTable);
      BeforeActions.scenario.write("BigQuery Target table: " + bqTargetTable + " is deleted successfully");
      bqTargetTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BigQuery Target Table: " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Before(order = 1, value = "@FILE_PATH")
  public static void setFileAbsolutePath() {

    if (firstFileSinkTestFlag) {
      PluginPropertyUtils.addPluginProp("expectedOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("expectedOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("expectedOutputFile1", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("expectedOutputFile1")).getPath()).toString());

      firstFileSinkTestFlag = false;
    }
  }

  @Before(order = 1, value = "@BQ_SOURCE_TABLE")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    String bqSourceDataset = System.getenv("SALESFORCE_MARKETING_DATASET");
    String bqSourceTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("bqsource.table", bqSourceTable);
    storeid = "SFMCStoreid" + RandomStringUtils.randomNumeric(5);
    String storestate = "Delhi";
    String storename = "Store" + RandomStringUtils.randomNumeric(2);


    BigQueryClient.getSoleQueryResult("create table `" + projectId + "." + bqSourceDataset + "." +
                                        bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ STRUCT('" + storeid + "' " +
                                        "AS storeid,  '" + storestate + "' AS " +
                                        "storestate, '" + storename + "'  AS storename)])");

    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_SOURCE_UPDATE")
  public static void createTempSourceBQTableToUpdate() throws IOException, InterruptedException {
    String bqSourceDataset = System.getenv("SALESFORCE_MARKETING_DATASET");
    String bqSourceTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    PluginPropertyUtils.addPluginProp("bqsource.table", bqSourceTable);

    storeid = PluginPropertyUtils.pluginProp("StoresId");
    String storestate = "UpdatedStoreState" + RandomStringUtils.randomNumeric(5);
    String storename = "UpdatedStoreName" + RandomStringUtils.randomNumeric(2);


    BigQueryClient.getSoleQueryResult("create table `" + projectId + "." + bqSourceDataset + "." +
                                        bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ STRUCT('" + storeid + "' " +
                                        "AS storeid,  '" + storestate + "' AS " +
                                        "storestate, '" + storename + "'  AS storename)])");

    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_CLEANUP")
  public void deleteTempSourceBQTable() throws IOException, InterruptedException {
    String bqSourceTable = PluginPropertyUtils.pluginProp("bqsource.table");
    try {
      dropBqTable(bqSourceTable);
      BeforeActions.scenario.write("BigQuery Source table: " + bqSourceTable + " is deleted successfully");
      bqSourceTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BigQuery Source Table: " + bqSourceTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Drops the Table using the values of ProjectId and Dataset from Environment Variables.
   */
  private static void dropBqTable(String table) throws IOException, InterruptedException {
    String projectId = System.getenv("PROJECT_ID");
    String datasetName = System.getenv("SALESFORCE_MARKETING_DATASET");
    String dropQuery = "DROP TABLE `" + projectId + "." + datasetName + "." + table + "`";
    BigQueryClient.getSoleQueryResult(dropQuery);
  }
}
