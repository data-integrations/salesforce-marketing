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
import org.junit.Assert;
import org.junit.AssumptionViolatedException;
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


  protected static final String CLIENT_ID = "clientId";
  protected static final String CLIENT_SECRET = "clientSecret";
  protected static final String AUTH_ENDPOINT = "authEndPoint";
  protected static final String SOAP_ENDPOINT = "soapEndPoint";
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudInputFormat.class);
  private MarketingCloudSourceConfig config;

  @Before
  public void initializeTests() {
    try {
      config = Mockito.spy(new MarketingCloudSourceConfig("referenceName", "queryMode",
        "objectName",
        "dataExtensionKey", "objectList",
        "dataExtensionKeys", "tableNameField",
        "filter", CLIENT_ID, CLIENT_SECRET, "restEndPoint", AUTH_ENDPOINT, SOAP_ENDPOINT));

    } catch (AssumptionViolatedException e) {
      LOG.warn("MarketingCloud batch Source tests are skipped. ");
      throw e;
    }
  }

  @Test
  public void testFetchTableInfoWDtaExtension() throws Exception {
    MarketingCloudSourceConfig config = new MarketingCloudSourceConfig("referenceName", "queryMode",
      "objectName",
      "dataExtensionKey", "objectList",
      "dataExtensionKeys", "tableNameField",
      "filter", CLIENT_ID, CLIENT_SECRET, "restEndPoint", AUTH_ENDPOINT, SOAP_ENDPOINT);
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
  public void testFetchTableInfoWMultiObject() throws Exception {
    MarketingCloudSourceConfig config = new MarketingCloudSourceConfig("referenceName", "queryMode",
      "objectName",
      "dataExtensionKey", "Data Extension,Unsub Event",
      "dataExtensionKeys1,dataExtensionKey2", "tableNameField",
      "filter", CLIENT_ID, CLIENT_SECRET, "restEndPoint", AUTH_ENDPOINT, SOAP_ENDPOINT);
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
}
