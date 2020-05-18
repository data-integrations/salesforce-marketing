/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.plugin.sfmc.source.util.SalesforceConstants;
import io.cdap.plugin.sfmc.source.util.SalesforceObjectInfo;
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
public class SalesforceInputFormat extends InputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceInputFormat.class);

  /**
   * Configure the input format to read tables from Salesforce. Should be called from the mapreduce client.
   *
   * @param jobConfig the job configuration
   * @param mode the query mode
   * @param conf the plugin conf
   * @param fetchRecordCount the flag to decide whether to fetch record count or not
   * @return Collection of SalesforceObjectInfo containing table and schema.
   */
  public static List<SalesforceObjectInfo> setInput(Configuration jobConfig, SourceQueryMode mode,
                                                    SalesforceSourceConfig conf, boolean fetchRecordCount) {
    SalesforceJobConfiguration jobConf = new SalesforceJobConfiguration(jobConfig);
    jobConf.setPluginConfiguration(conf);

    //Depending on the selected objects in the conf, get the schema for each object as SalesforceObjectInfo
    List<SalesforceObjectInfo> tableInfos = fetchTableInfo(mode, conf, fetchRecordCount);

    jobConf.setTableInfos(tableInfos);

    LOG.debug("setInput::tableInfos = {}", tableInfos.size());
    return tableInfos;
  }

  /**
   * Depending on conf value fetch the list of fields for each object and create schema object.
   *
   * @param mode the query mode
   * @param conf the plugin conf
   * @param fetchRecordCount the flag to decide whether to fetch record count or not
   * @return Collection of SalesforceObjectInfo containing table and schema.
   */
  private static List<SalesforceObjectInfo> fetchTableInfo(SourceQueryMode mode, SalesforceSourceConfig conf,
                                                           boolean fetchRecordCount) {
    try {
      SalesforceClient client = SalesforceClient.create(conf.getClientId(), conf.getClientSecret(),
        conf.getAuthEndpoint(), conf.getSoapEndpoint());

      boolean failOnError = conf.isFailOnError();

      //When mode = SingleObject, fetch fields for the object selected in plugin config
      if (mode == SourceQueryMode.SINGLE_OBJECT) {
        SalesforceObjectInfo tableInfo = getTableMetaData(conf.getObject(), conf.getDataExtensionKey(), client,
          failOnError, fetchRecordCount);
        return (tableInfo == null) ? Collections.emptyList() : Collections.singletonList(tableInfo);
      }

      //When mode = MultiObject, get the list of objects provided in plugin config and the fetch fields for each of
      //then objects.
      List<SalesforceObjectInfo> tableInfos = new ArrayList<>();
      List<SourceObject> objectList = conf.getObjectList();

      for (SourceObject object : objectList) {
        SalesforceObjectInfo tableInfo = null;

        if (object == SourceObject.DATA_EXTENSION) {
          //if the object = Data Extension then get the list of data extension keys and then fetch fields for each of
          //the data extension objects.
          List<String> dataExtensionKeys = Util.splitToList(conf.getDataExtensionKeys(), ',');
          for (String dataExtensionKey : dataExtensionKeys) {
            tableInfo = getTableMetaData(object, dataExtensionKey, client, failOnError, fetchRecordCount);
            if (tableInfo == null) {
              continue;
            }
            tableInfos.add(tableInfo);
          }
        } else {
          tableInfo = getTableMetaData(object, "", client, failOnError, fetchRecordCount);
          if (tableInfo != null) {
            tableInfos.add(tableInfo);
          }
        }
      }

      return tableInfos;
    } catch (Exception e) {
      if (conf.isFailOnError()) {
        LOG.error("Error in fetchTableInfo()", e);
      } else {
        LOG.warn("Failed in fetchTableInfo()", e);
      }
      return Collections.emptyList();
    }
  }

  /**
   * Fetch the fields for passed object.
   */
  private static SalesforceObjectInfo getTableMetaData(SourceObject object, String dataExtensionKey,
                                                       SalesforceClient client, boolean failOnError,
                                                       boolean fetchRecordCount) {
    try {
      if (object == SourceObject.DATA_EXTENSION) {
        return client.fetchDataExtensionSchema(dataExtensionKey, fetchRecordCount);
      } else {
        return client.fetchObjectSchema(object, fetchRecordCount);
      }
    } catch (Exception e) {
      if (failOnError) {
        LOG.error("Error in getTableMetaData()", e);
      } else {
        LOG.warn("Failed in getTableMetaData()", e);
      }
      return null;
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    SalesforceJobConfiguration jobConfig = new SalesforceJobConfiguration(jobContext.getConfiguration());
    SalesforceSourceConfig pluginConf = jobConfig.getPluginConf();

    List<SalesforceObjectInfo> tableInfos = jobConfig.getTableInfos();
    List<InputSplit> resultSplits = new ArrayList<>();

    for (SalesforceObjectInfo tableInfo : tableInfos) {
      String tableKey = tableInfo.getObject().name();
      String tableName = tableInfo.getTableName();
      int totalRecords = tableInfo.getRecordCount();
      if (totalRecords <= SalesforceConstants.MAX_PAGE_SIZE) {
        //add single split for table and continue
        resultSplits.add(new SalesforceInputSplit(tableKey, tableName, 1, totalRecords));
        continue;
      }

      int pages = (tableInfo.getRecordCount() / SalesforceConstants.MAX_PAGE_SIZE) + 1;
      for (int page = 1; page <= pages; page++) {
        resultSplits.add(new SalesforceInputSplit(tableKey, tableName, page, SalesforceConstants.MAX_PAGE_SIZE));
      }
    }

    LOG.debug("# of split = {}", resultSplits.size());

    return resultSplits;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit inputSplit,
                                                                         TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    SalesforceJobConfiguration jobConfig = new SalesforceJobConfiguration(taskAttemptContext.getConfiguration());
    SalesforceSourceConfig pluginConf = jobConfig.getPluginConf();

    return new SalesforceRecordReader(pluginConf);
  }
}
