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


import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class MarketingCloudSourceTest {

  protected static final String CLIENT_ID = System.getProperty("sfmc.test.clientId");
  protected static final String CLIENT_SECRET = System.getProperty("sfmc.test.clientSecret");
  protected static final String AUTH_ENDPOINT = System.getProperty("sfmc.test.authEndpoint");
  protected static final String SOAP_ENDPOINT = System.getProperty("sfmc.test.soapEndpoint");
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudSource.class);
  private MarketingCloudSource marketingCloudSource;
  private MarketingCloudSourceConfig marketingCloudSourceConfig;

  @Before
  public void initialize() {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, AUTH_ENDPOINT, SOAP_ENDPOINT);
      SourceQueryMode mode = SourceQueryMode.SINGLE_OBJECT;
      SourceObject object = SourceObject.DATA_EXTENSION;
      marketingCloudSourceConfig = SalesforceSourceConfigHelper.newConfigBuilder().
        setReferenceName("referenceName")
        .setAuthEndpoint(AUTH_ENDPOINT)
        .setClientId(CLIENT_ID)
        .setClientSecret(CLIENT_SECRET)
        .setSoapEndpoint(SOAP_ENDPOINT)
        .setRestEndpoint("restEndPoint")
        .setDataExtensionKey("DE")
        .setQueryMode("Single Object")
        .setDataExtensionKeys(null)
        .setTableNameField("tableNameField")
        .setFilter("")
        .setObjectName("Data Extension")
        .setObjectList(null)
        .build();
      marketingCloudSource = new MarketingCloudSource(marketingCloudSourceConfig);
    } catch (AssumptionViolatedException e) {
      LOG.warn("MarketingCloud batch Source tests are skipped. ");
      throw e;
    }
  }

  @Test
  public void testConfigurePipeline() {
    Schema inputSchema = null;
    Map<String, Object> plugins = new HashMap<>();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema, plugins);
    marketingCloudSource.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }


  @Test
  public void testPrepareRun() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(mockArguments);
    marketingCloudSource.prepareRun(context);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }


}







