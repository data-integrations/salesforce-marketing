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
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
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
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudSource.class);
  private MarketingCloudSource marketingCloudSource;
  private MarketingCloudSourceConfig marketingCloudSourceConfig;

  @Before
  public void initialize() {
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
      Assert.fail("Exception is not thrown if valid credentials are provided");
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
    marketingCloudSource.configurePipeline(mockPipelineConfigurer);
    Assert.assertNull(mockPipelineConfigurer.getOutputSchema());
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testPrepareRunWithInvalidCredentials() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(mockArguments);
    try {
      marketingCloudSource.prepareRun(context);
      Assert.fail("Exception is not thrown if valid credentials are provided");
    } catch (ValidationException e) {
      Assert.assertEquals("Unable to connect to Salesforce Instance.", e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testPrepareRun() throws Exception {
    SourceObject object = SourceObject.TRACKING_UNSUB_EVENT;
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
    MarketingCloudClient client = PowerMockito.mock(MarketingCloudClient.class);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.whenNew(MarketingCloudClient.class).withArguments(Mockito.any()).thenReturn(client);
    PowerMockito.mockStatic(MarketingCloudInputFormat.class);
    BatchSourceContext batchSourceContext = Mockito.mock(BatchSourceContext.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(batchSourceContext.getFailureCollector()).thenReturn(mockFailureCollector);
    PowerMockito.when(client.fetchDataExtensionSchema("dataExtensionKey")).thenReturn(sObjectInfo);
    marketingCloudSource.prepareRun(batchSourceContext);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }
}
