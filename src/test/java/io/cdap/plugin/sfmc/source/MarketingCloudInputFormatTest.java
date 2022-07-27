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
package io.cdap.plugin.sfmc.source;

import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETConfiguration;
import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MarketingCloudClient.class, MarketingCloudInputFormat.class, ETClient.class})
public class MarketingCloudInputFormatTest {
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String AUTH_ENDPOINT = "authEndPoint";
  private static final String SOAP_ENDPOINT = "soapEndPoint";
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudInputFormat.class);
  private MarketingCloudSourceConfig config;

  @Before
  public void initializeTests() {
    config = Mockito.spy(new MarketingCloudSourceConfig("referenceName", "queryMode",
                                                        "objectName",
                                                        "dataExtensionKey", "objectList",
                                                        "dataExtensionKeys", "tableNameField",
                                                        "filter", CLIENT_ID, CLIENT_SECRET,
                                                         AUTH_ENDPOINT, SOAP_ENDPOINT));
      config = Mockito.spy(new MarketingCloudSourceConfig("referenceName", "queryMode",
        "objectName",
        "dataExtensionKey", "objectList",
        "dataExtensionKeys", "tableNameField",
        "filter", CLIENT_ID, CLIENT_SECRET,  AUTH_ENDPOINT, SOAP_ENDPOINT));

    config = Mockito.spy(new MarketingCloudSourceConfig("referenceName", "queryMode",
                                                        "objectName",
                                                        "dataExtensionKey", "objectList",
                                                        "dataExtensionKeys", "tableNameField",
                                                        "filter", CLIENT_ID, CLIENT_SECRET,
                                                         AUTH_ENDPOINT, SOAP_ENDPOINT));
  }

  @Test
  public void testFetchTableInfoWithDtaExtension() throws Exception {
    MarketingCloudSourceConfig config = new MarketingCloudSourceConfig("referenceName", "queryMode",
                                                                       "objectName",
                                                                       "dataExtensionKey", "objectList",
                                                                       "dataExtensionKeys",
                                                                       "tableNameField", "filter",
                                                                       CLIENT_ID, CLIENT_SECRET,
                                                                        AUTH_ENDPOINT, SOAP_ENDPOINT);
    SourceQueryMode mode = SourceQueryMode.SINGLE_OBJECT;
    SourceObject object = SourceObject.DATA_EXTENSION;
    ETConfiguration conf = Mockito.spy(new ETConfiguration());
    conf.set("clientId", CLIENT_ID);
    conf.set("clientSecret", CLIENT_SECRET);
    conf.set("authEndpoint", AUTH_ENDPOINT);
    conf.set("soapEndpoint", SOAP_ENDPOINT);
    conf.set("useOAuth2Authentication", "true");
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);
    MarketingCloudObjectInfo info = new MarketingCloudObjectInfo(object, columns);
    List<MarketingCloudObjectInfo> tableInfo = new ArrayList<>();
    tableInfo.add(info);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.spy(new MarketingCloudClient(new ETClient(conf)));
    Configuration jobConfig = Mockito.mock(Configuration.class);
    PowerMockito.mockStatic(MarketingCloudInputFormat.class);
    PowerMockito.when(MarketingCloudInputFormat.fetchTableInfo(mode, config)).thenReturn(tableInfo);
    MarketingCloudInputFormat.setInput(jobConfig, mode, config);
    Assert.assertEquals(1, MarketingCloudInputFormat.fetchTableInfo(mode, config).size());
  }

  @Test
  public void testFetchTableInfoWithMultiObject() throws Exception {
    MarketingCloudSourceConfig config = new MarketingCloudSourceConfig("referenceName", "queryMode",
                                                                       "objectName", "dataExtensionKey",
                                                                       "Data Extension,Unsub Event",
                                                                       "dataExtensionKeys1,dataExtensionKey2",
                                                                       "tableNameField",
                                                                       "filter", CLIENT_ID, CLIENT_SECRET,
                                                                        AUTH_ENDPOINT, SOAP_ENDPOINT);
    SourceQueryMode mode = SourceQueryMode.MULTI_OBJECT;
    SourceObject object = SourceObject.DATA_EXTENSION;
    ETConfiguration conf = Mockito.spy(new ETConfiguration());
    conf.set("clientId", CLIENT_ID);
    conf.set("clientSecret", CLIENT_SECRET);
    conf.set("authEndpoint", AUTH_ENDPOINT);
    conf.set("soapEndpoint", SOAP_ENDPOINT);
    conf.set("useOAuth2Authentication", "true");
    List<SourceObject> list = new ArrayList<>();
    SourceObject object1 = SourceObject.DATA_EXTENSION;
    SourceObject object2 = SourceObject.TRACKING_UNSUB_EVENT;
    list.add(object1);
    list.add(object2);
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column1 = new MarketingCloudColumn("sys_created_by", "string");
    MarketingCloudColumn column2 = new MarketingCloudColumn("sys_updated_by", "string");
    columns.add(column1);
    columns.add(column2);
    MarketingCloudObjectInfo info = new MarketingCloudObjectInfo(object, columns);
    List<MarketingCloudObjectInfo> tableInfos = new ArrayList<>();
    tableInfos.add(info);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.spy(new MarketingCloudClient(new ETClient(conf)));
    Mockito.when(config.getObjectList()).thenReturn(list);
    Configuration jobConfig = Mockito.mock(Configuration.class);
    PowerMockito.spy(MarketingCloudInputFormat.class);
    PowerMockito.when(MarketingCloudInputFormat.fetchTableInfo(mode, config)).thenReturn(tableInfos);
    MarketingCloudInputFormat.setInput(jobConfig, mode, config);
    Assert.assertEquals(1, MarketingCloudInputFormat.fetchTableInfo(mode, config).size());
  }

  @Test
  public void testCreateRecordReader() throws Exception {
    InputSplit inputSplit = Mockito.mock(InputSplit.class);
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    MarketingCloudJobConfiguration jobConfiguration = Mockito.mock(MarketingCloudJobConfiguration.class);
    PowerMockito.whenNew(MarketingCloudJobConfiguration.class).withArguments(Mockito.any())
      .thenReturn(jobConfiguration);
    MarketingCloudInputFormat marketingCloudInputFormat = new MarketingCloudInputFormat();
    Assert.assertNotNull(marketingCloudInputFormat.createRecordReader(inputSplit, taskAttemptContext));
  }

  @Test
  public void testGetSplits() throws Exception {
    List<MarketingCloudObjectInfo> tableInfos = new ArrayList<>();
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column = new MarketingCloudColumn("name", "String");
    columns.add(column);
    MarketingCloudObjectInfo info = new MarketingCloudObjectInfo(SourceObject.TRACKING_UNSUB_EVENT, columns);
    tableInfos.add(info);
    JobContext jobContext = Mockito.mock(JobContext.class);
    MarketingCloudInputFormat inputFormat = new MarketingCloudInputFormat();
    MarketingCloudJobConfiguration jobConfiguration = Mockito.mock(MarketingCloudJobConfiguration.class);
    PowerMockito.whenNew(MarketingCloudJobConfiguration.class).withArguments(Mockito.any())
      .thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getTableInfos()).thenReturn(tableInfos);
    MarketingCloudSourceConfig config = Mockito.mock(MarketingCloudSourceConfig.class);
    Mockito.when(jobConfiguration.getPluginConf()).thenReturn(config);
    Assert.assertNotNull(inputFormat.getSplits(jobContext));
  }
}
