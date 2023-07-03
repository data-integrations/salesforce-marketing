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


import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETDataExtension;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ETClient.class, ClassLoader.class, MarketingCloudClient.class, MarketingCloudInputFormat.class})
public class MarketingCloudSourceTest {
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String AUTH_ENDPOINT = "authEndPoint";
  private static final String SOAP_ENDPOINT = "soapEndPoint";
  private MarketingCloudSource marketingCloudSource;
  private MarketingCloudSourceConfig marketingCloudSourceConfig;

  @Before
  public void initialize() throws ETSdkException {
    marketingCloudSourceConfig = SalesforceSourceConfigHelper.newConfigBuilder().
      setReferenceName("referenceName")
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .setDataExtensionKey("DE")
      .setQueryMode("Single Object")
      .setDataExtensionKeys(null)
      .setTableNameField("tableNameField")
      .setFilter("")
      .setObjectName("Data Extension")
      .setObjectList(null)
      .build();
    marketingCloudSource = new MarketingCloudSource(marketingCloudSourceConfig);
  }

  @Test
  public void testConfigurePipelineWithInvalidCredentials() {
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    try {
      marketingCloudSource.configurePipeline(mockPipelineConfigurer);
    } catch (ValidationException e) {
      Assert.assertEquals("Unable to connect to Salesforce Instance.", e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testConfigurePipeline() throws Exception {
    SourceObject object = SourceObject.TRACKING_UNSUB_EVENT;
    Map<String, Object> plugins = new HashMap<>();
    Map<String, Object> map = new HashMap<>();
    List<Map<String, Object>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
    List<MarketingCloudObjectInfo> list = new ArrayList<>();
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column = new MarketingCloudColumn("price", "String");
    columns.add(column);
    MarketingCloudObjectInfo sObjectInfo = new MarketingCloudObjectInfo(object
      , columns);
    list.add(sObjectInfo);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    MarketingCloudClient client = PowerMockito.mock(MarketingCloudClient.class);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.whenNew(MarketingCloudClient.class).withArguments(Mockito.any()).thenReturn(client);
    PowerMockito.when(client.fetchDataExtensionSchema("dataExtensionKey")).thenReturn(sObjectInfo);
    List<? extends ETApiObject> etApiObjects = new ArrayList<>();
    List<ETDataExtension> etDataExtensions = new ArrayList<>();
    ETApiObject row = new ETDataExtension();
    row.setId("id");
    ETDataExtension etDataExtension = new ETDataExtension();
    etDataExtension.setKey("DE");
    etDataExtension.setName("DE");
    etDataExtensions.add(etDataExtension);
    etApiObjects = etDataExtensions;
    ETResponse<ETDataExtension> etResponse = Mockito.mock(ETResponse.class);
    PowerMockito.when(client.retrieveDataExtensionKeys()).thenReturn(etResponse);
    Mockito.doReturn(etApiObjects).when(etResponse).getObjects();
    marketingCloudSource.configurePipeline(mockPipelineConfigurer);
    Assert.assertNull(mockPipelineConfigurer.getOutputSchema());
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testPrepareRunWithInvalidCredentials() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(mockArguments);
    try {
      marketingCloudSource.prepareRun(context);
    } catch (ValidationException e) {
      Assert.assertEquals("Unable to connect to Salesforce Instance.", e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testPrepareRun() throws Exception {
    SourceQueryMode mode = marketingCloudSourceConfig.getQueryMode();
    Configuration configuration = new Configuration();
    Map<String, Object> map = new HashMap<>();
    List<Map<String, Object>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
    List<MarketingCloudObjectInfo> list = new ArrayList<>();
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column = new MarketingCloudColumn("price", "String");
    columns.add(column);
    MarketingCloudObjectInfo sObjectInfo = Mockito.mock(MarketingCloudObjectInfo.class);
    list.add(sObjectInfo);
    MarketingCloudClient client = PowerMockito.mock(MarketingCloudClient.class);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.whenNew(MarketingCloudClient.class).withArguments(Mockito.any()).thenReturn(client);
    PowerMockito.mockStatic(MarketingCloudInputFormat.class);
    BatchSourceContext batchSourceContext = Mockito.mock(BatchSourceContext.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(batchSourceContext.getFailureCollector()).thenReturn(mockFailureCollector);
    PowerMockito.when(client.fetchDataExtensionSchema("dataExtensionKey")).thenReturn(sObjectInfo);
    Mockito.when(MarketingCloudInputFormat.setInput(configuration, mode, marketingCloudSourceConfig)).thenReturn(list);
    marketingCloudSource.prepareRun(batchSourceContext);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }
  @Test
  public void testConfigurePipelineWMapReduce() throws Exception {
    StageConfigurer stageConfigurer = PowerMockito.mock(StageConfigurer.class);
    FailureCollector failureCollector = PowerMockito.mock(FailureCollector.class);
    PipelineConfigurer pipelineConfigurer = PowerMockito.mock(PipelineConfigurer.class);
    PowerMockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(stageConfigurer);
    PowerMockito.when(stageConfigurer.getFailureCollector()).thenReturn(failureCollector);
    Mockito.when(pipelineConfigurer.getEngine()).thenReturn(Engine.MAPREDUCE);
    SourceObject object = SourceObject.TRACKING_UNSUB_EVENT;
    Map<String, Object> plugins = new HashMap<>();
    Map<String, Object> map = new HashMap<>();
    List<Map<String, Object>> result = new ArrayList<>();
    map.put("key", "value");
    result.add(map);
    List<MarketingCloudObjectInfo> list = new ArrayList<>();
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column = new MarketingCloudColumn("price", "String");
    columns.add(column);
    MarketingCloudObjectInfo sObjectInfo = new MarketingCloudObjectInfo(object
      , columns);
    list.add(sObjectInfo);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    MarketingCloudClient client = PowerMockito.mock(MarketingCloudClient.class);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.whenNew(MarketingCloudClient.class).withArguments(Mockito.any()).thenReturn(client);
    PowerMockito.when(client.fetchDataExtensionSchema("dataExtensionKey")).thenReturn(sObjectInfo);
    marketingCloudSource.configurePipeline(mockPipelineConfigurer);
    Assert.assertNull(mockPipelineConfigurer.getOutputSchema());
    marketingCloudSource.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testPrepareRunMarketingObjectInfo() throws Exception {
    PowerMockito.mockStatic(MarketingCloudInputFormat.class);
    MockFailureCollector failureCollector = PowerMockito.mock(MockFailureCollector.class);
    StageConfigurer stageConfigurer = PowerMockito.mock(StageConfigurer.class);
    PowerMockito.when(stageConfigurer.getFailureCollector()).thenReturn(failureCollector);
    MarketingCloudObjectInfo cloudObjectInfo = Mockito.mock(MarketingCloudObjectInfo.class);
    Configuration hConf = new Configuration();
    SourceQueryMode mode = marketingCloudSourceConfig.getQueryMode();
    SourceObject object = SourceObject.DATA_EXTENSION;
    MarketingCloudColumn marketingCloudColumn = new MarketingCloudColumn("fieldName", "typeName");
    List<MarketingCloudColumn> tables = new ArrayList<>();
    tables.add(marketingCloudColumn);
    MarketingCloudObjectInfo marketingCloudObjectInfo = new MarketingCloudObjectInfo(object, tables);
    Collection<MarketingCloudObjectInfo> list = new ArrayList<>();
    list.add(marketingCloudObjectInfo);
    MarketingCloudClient client = PowerMockito.mock(MarketingCloudClient.class);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.whenNew(MarketingCloudClient.class).withArguments(Mockito.any()).thenReturn(client);
    BatchSourceContext batchSourceContext = Mockito.mock(BatchSourceContext.class);
    Mockito.when(batchSourceContext.getFailureCollector()).thenReturn(failureCollector);
    PowerMockito.when(MarketingCloudInputFormat.setInput(hConf, mode, marketingCloudSourceConfig)).thenReturn(
      (List<MarketingCloudObjectInfo>) list);
    marketingCloudSource.prepareRun(batchSourceContext);
    Assert.assertEquals(0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testTransform() {
    NullWritable nullWritable = Mockito.mock(NullWritable.class);
    StructuredRecord structuredRecord = Mockito.mock(StructuredRecord.class);
    KeyValue<NullWritable, StructuredRecord> input = new KeyValue<>(nullWritable, structuredRecord);
    Emitter<StructuredRecord> emitter = new MockEmitter<>();
    marketingCloudSource.transform(input, emitter);
  }
}
