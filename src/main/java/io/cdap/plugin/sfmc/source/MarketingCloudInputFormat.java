/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.sfmc.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import io.cdap.plugin.sfmc.source.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Salesforce input format.
 */
public class MarketingCloudInputFormat extends InputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudInputFormat.class);

  /**
   * Configure the input format to read tables from Salesforce. Should be called from the mapreduce client.
   *
   * @param jobConfig the job configuration
   * @param mode      the query mode
   * @param conf      the plugin conf
   * @return Collection of MarketingCloudObjectInfo containing table and schema.
   */
  public static List<MarketingCloudObjectInfo> setInput(Configuration jobConfig, SourceQueryMode mode,
                                                        MarketingCloudSourceConfig conf) {
    MarketingCloudJobConfiguration jobConf = new MarketingCloudJobConfiguration(jobConfig);
    jobConf.setPluginConfiguration(conf);

    //Depending on the selected objects in the conf, get the schema for each object as MarketingCloudObjectInfo
    List<MarketingCloudObjectInfo> tableInfos = fetchTableInfo(mode, conf);

    jobConf.setTableInfos(tableInfos);

    LOG.debug("setInput::tableInfos = {}", tableInfos.size());
    return tableInfos;
  }

  /**
   * Depending on conf value fetch the list of fields for each object and create schema object.
   *
   * @param mode the query mode
   * @param conf the plugin conf
   * @return Collection of MarketingCloudObjectInfo containing table and schema.
   */
  static List<MarketingCloudObjectInfo> fetchTableInfo(SourceQueryMode mode, MarketingCloudSourceConfig conf) {
    try {
      MarketingCloudClient client = MarketingCloudClient.create(conf.getConnection().getClientId(),
                                                                conf.getConnection().getClientSecret(),
                                                                conf.getConnection().getAuthEndpoint(),
                                                                conf.getConnection().getSoapEndpoint());

      //When mode = SingleObject, fetch fields for the object selected in plugin config
      if (mode == SourceQueryMode.SINGLE_OBJECT) {
        MarketingCloudObjectInfo tableInfo = getTableMetaData(conf.getObject(), conf.getDataExtensionKey(), client);
        return (tableInfo == null) ? Collections.emptyList() : Collections.singletonList(tableInfo);
      }

      //When mode = MultiObject, get the list of objects provided in plugin config and the fetch fields for each of
      //then objects.
      List<MarketingCloudObjectInfo> tableInfos = new ArrayList<>();
      List<SourceObject> objectList = conf.getObjectList();

      for (SourceObject object : objectList) {
        MarketingCloudObjectInfo tableInfo = null;

        if (object == SourceObject.DATA_EXTENSION) {
          //if the object = Data Extension then get the list of data extension keys and then fetch fields for each of
          //the data extension objects.
          List<String> dataExtensionKeys = Util.splitToList(conf.getDataExtensionKeys(), ',');
          for (String dataExtensionKey : dataExtensionKeys) {
            tableInfo = getTableMetaData(object, dataExtensionKey, client);
            if (tableInfo == null) {
              continue;
            }
            tableInfos.add(tableInfo);
          }
        } else {
          tableInfo = getTableMetaData(object, "", client);
          if (tableInfo != null) {
            tableInfos.add(tableInfo);
          }
        }
      }

      return tableInfos;
    } catch (Exception e) {
      LOG.error("Error retrieving object schema. Check object exists.", e);
      return Collections.emptyList();
    }
  }

  /**
   * Fetch the fields for passed object.
   */
  private static MarketingCloudObjectInfo getTableMetaData(SourceObject object, String dataExtensionKey,
                                                           MarketingCloudClient client) {
    try {
      if (object == SourceObject.DATA_EXTENSION) {
        return client.fetchDataExtensionSchema(dataExtensionKey);
      } else {
        return client.fetchObjectSchema(object);
      }
    } catch (Exception e) {
      LOG.error("Error retrieving object schema. Check Object type or DataExtension is valid. ", e);
      return null;
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    MarketingCloudJobConfiguration jobConfig = new MarketingCloudJobConfiguration(jobContext.getConfiguration());
    MarketingCloudSourceConfig pluginConf = jobConfig.getPluginConf();

    List<MarketingCloudObjectInfo> tableInfos = jobConfig.getTableInfos();
    List<InputSplit> resultSplits = new ArrayList<>();

    for (MarketingCloudObjectInfo tableInfo : tableInfos) {
      String tableKey = tableInfo.getObject().name();
      String tableName = tableInfo.getTableName();
      resultSplits.add(new MarketingCloudInputSplit(tableKey, tableName));
    }

    LOG.debug("# of split = {}", resultSplits.size());

    return resultSplits;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit inputSplit, TaskAttemptContext
    taskAttemptContext) throws IOException, InterruptedException {
    MarketingCloudJobConfiguration jobConfig = new MarketingCloudJobConfiguration(taskAttemptContext.
                                                                                    getConfiguration());
    MarketingCloudSourceConfig pluginConf = jobConfig.getPluginConf();

    return new MarketingCloudRecordReader(pluginConf);
  }
}
