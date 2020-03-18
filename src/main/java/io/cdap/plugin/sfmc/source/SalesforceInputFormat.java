/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sfmc.source.apiclient.SalesforceTableAPIClientImpl;

import io.cdap.plugin.sfmc.source.apiclient.SalesforceTableDataResponse;
import io.cdap.plugin.sfmc.source.util.SalesforceColumn;
import io.cdap.plugin.sfmc.source.util.SalesforceObjectInfo;
import io.cdap.plugin.sfmc.source.util.SchemaBuilder;
import io.cdap.plugin.sfmc.source.util.SourceObjectMode;
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

import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PAGE_SIZE;

/**
 * ServiceNow input format
 */
public class SalesforceInputFormat extends InputFormat<NullWritable, StructuredRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(SalesforceInputFormat.class);

    /**
     * Configure the input format to read tables from ServiceNow. Should be called from the mapreduce client.
     *
     * @param jobConfig the job configuration
     * @param mode
     * @param conf      the database conf
     * @return Collection of SalesforceObjectInfo containing table and schema.
     */
    public static List<SalesforceObjectInfo> setInput(Configuration jobConfig, SourceObjectMode mode,
                                                      SalesforceSourceConfig conf) {
        SalesforceJobConfiguration jobConf = new SalesforceJobConfiguration(jobConfig);
        jobConf.setPluginConfiguration(conf);

        //Depending on conf value fetch the list of fields for each table and create schema object
        //return the schema object for each table as SalesforceObjectInfo
        List<SalesforceObjectInfo> tableInfos = fetchTableInfo(mode, conf);

        jobConf.setTableInfos(tableInfos);

        return tableInfos;
    }

    private static List<SalesforceObjectInfo> fetchTableInfo(SourceObjectMode mode, SalesforceSourceConfig conf) {
        //When mode = Table, fetch details from the table name provided in plugin config
        if (mode == SourceObjectMode.SINGLEOBJECT) {
            SalesforceObjectInfo tableInfo = getTableMetaData(conf.getObjectName(), conf);
            return (tableInfo == null) ? Collections.emptyList() : Collections.singletonList(tableInfo);
        }

        //When mode = Reporting, get the list of tables for application name provided in plugin config
        //and then fetch details from each of the tables.
        List<SalesforceObjectInfo> tableInfos = new ArrayList<>();

        List<String> tableNames = conf.getApplicationName().getTableNames();
        for (String tableName : tableNames) {
            SalesforceObjectInfo tableInfo = getTableMetaData(tableName, conf);
            if (tableInfo == null) {
                continue;
            }
            tableInfos.add(tableInfo);
        }

        return tableInfos;
    }

    private static SalesforceObjectInfo getTableMetaData(String tableName, SalesforceSourceConfig conf) {
        //Call API to fetch first record from the table
        SalesforceTableAPIClientImpl restApi = new SalesforceTableAPIClientImpl(conf);
        SalesforceTableDataResponse response = null;
        //SalesforceTableDataResponse response = restApi.fetchTableSchema(tableName,
        // conf.getStartDate(), conf.getEndDate(),
        //  true);
        if (response == null) {
            return null;
        }

        List<SalesforceColumn> columns = response.getColumns();
        if (columns == null || columns.isEmpty()) {
            return null;
        }

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        Schema schema = schemaBuilder.constructSchema(tableName, columns);
        LOG.debug("table {}, rows = {}", tableName, response.getTotalRecordCount());
        return new SalesforceObjectInfo(tableName, schema, response.getTotalRecordCount());
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        SalesforceJobConfiguration jobConfig = new SalesforceJobConfiguration(jobContext.getConfiguration());
        SalesforceSourceConfig pluginConf = jobConfig.getPluginConf();

        List<SalesforceObjectInfo> tableInfos = jobConfig.getTableInfos();
        List<InputSplit> resultSplits = new ArrayList<>();

        for (SalesforceObjectInfo tableInfo : tableInfos) {
            String tableName = tableInfo.getTableName();
            int totalRecords = tableInfo.getRecordCount();
            if (totalRecords < PAGE_SIZE) {
                //add single split for table and continue
                resultSplits.add(new SalesforceInputSplit(tableName, 0, totalRecords));
                continue;
            }

            int pages = (tableInfo.getRecordCount() / PAGE_SIZE) + 1;
            int offset = 0;
            int recordsOnPage = PAGE_SIZE;

            for (int page = 1; page <= pages; page++) {
                if (page == pages) {
                    recordsOnPage = totalRecords - offset;
                }
                resultSplits.add(new SalesforceInputSplit(tableName, offset, recordsOnPage));
                offset += PAGE_SIZE;
                recordsOnPage -= PAGE_SIZE;
            }
        }

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
