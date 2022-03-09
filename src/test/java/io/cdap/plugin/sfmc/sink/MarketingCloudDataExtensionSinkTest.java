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
package io.cdap.plugin.sfmc.sink;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;

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


public class MarketingCloudDataExtensionSinkTest {
  protected static final String CLIENT_ID = "clientId";
  protected static final String CLIENT_SECRET = "clientSecret";
  protected static final String AUTH_ENDPOINT = "authEndPoint";
  protected static final String SOAP_ENDPOINT = "soapEndPoint";
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudDataExtensionSink.class);
  private MarketingCloudConf marketingCloudConf;
  private MarketingCloudDataExtensionSink marketingCloudDataExtensionSink;

  @Before
  public void initialize() {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, AUTH_ENDPOINT, SOAP_ENDPOINT);
      Operation operation = Operation.UPDATE;
      marketingCloudConf = MarketingCloudConfHelper.newConfigBuilder().
        setReferenceName("referenceName")
        .setAuthEndpoint(AUTH_ENDPOINT)
        .setClientId(CLIENT_ID)
        .setClientSecret(CLIENT_SECRET)
        .setSoapEndpoint(SOAP_ENDPOINT)
        .setDataExtension("DE")
        .setMaxBatchSize(500)
        .setOperation("INSERT")
        .setColumnMapping("<key=value>").
        setFailOnError(false)
        .setTruncateText(false)
        .setReplaceWithSpaces(false)
        .build();
      marketingCloudDataExtensionSink = new MarketingCloudDataExtensionSink(marketingCloudConf);
    } catch (AssumptionViolatedException e) {
      LOG.warn("MarketingCloud batch sink tests are skipped. ");
      throw e;
    }
  }

  @Test
  public void testConfigurePipeline() {
    Schema inputSchema = null;
    Map<String, Object> plugins = new HashMap<>();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema, plugins);
    marketingCloudDataExtensionSink.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testConfigurePipelineWithEmptyClientId() {
    marketingCloudConf = MarketingCloudConfHelper.newConfigBuilder().
      setReferenceName("referenceName")
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setClientSecret(CLIENT_SECRET)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .setDataExtension("dataExtension")
      .setMaxBatchSize(500)
      .setOperation("INSERT")
      .setColumnMapping("<key=value>").
      setFailOnError(true)
      .setTruncateText(false)
      .setReplaceWithSpaces(false)
      .build();
    marketingCloudDataExtensionSink = new MarketingCloudDataExtensionSink(marketingCloudConf);
    Schema inputSchema = null;
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema, plugins);
    MockFailureCollector collector = new MockFailureCollector();
    try {
      marketingCloudDataExtensionSink.configurePipeline(mockPipelineConfigurer);
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation.",
        e.getMessage());
    }
  }

  @Test
  public void testPrepareRun() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSinkContext context = Mockito.mock(BatchSinkContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(mockArguments);
    marketingCloudDataExtensionSink.prepareRun(context);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testPrepareRunWInvalidClient() {
    Schema inputSchema = Schema.recordOf("record",
      Schema.Field.of("storeid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSinkContext batchSinkContext = Mockito.mock(BatchSinkContext.class);
    Mockito.when(batchSinkContext.getInputSchema()).thenReturn(inputSchema);
    Mockito.when(batchSinkContext.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(batchSinkContext.getArguments()).thenReturn(mockArguments);
    try {
      marketingCloudDataExtensionSink.prepareRun(batchSinkContext);
      mockFailureCollector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("Errors were encountered during validation.", e.getMessage());
    }
  }


  @Test
  public void testGetMappedSchema() {
    Schema inputSchema = Schema.recordOf("record",
      Schema.Field.of("storeid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    MockFailureCollector collector = new MockFailureCollector("SFMCSink");
    Map<String, String> columnMapping = new HashMap<>();
    columnMapping.put("price", "200");
    Assert.assertFalse(marketingCloudDataExtensionSink.getMappedSchema(columnMapping, inputSchema).isNullable());
    Assert.assertEquals(0, collector.getValidationFailures().size());

  }

//Todo

  @Test
  public void testGetOutputFormatConfiguration() {
    OutputFormatProvider outputFormatProvider = Mockito.mock(OutputFormatProvider.class);
    Map<String, String> outputConfig = new HashMap<>();
    outputConfig.put(DataExtensionOutputFormat.CLIENT_ID, marketingCloudConf.getClientId());
    outputConfig.put(DataExtensionOutputFormat.CLIENT_SECRET, marketingCloudConf.getClientSecret());
    outputConfig.put(DataExtensionOutputFormat.AUTH_ENDPOINT, marketingCloudConf.getAuthEndpoint());
    outputConfig.put(DataExtensionOutputFormat.SOAP_ENDPOINT, marketingCloudConf.getSoapEndpoint());
    outputConfig.put(DataExtensionOutputFormat.MAX_BATCH_SIZE, String.valueOf(marketingCloudConf.getMaxBatchSize()));
    outputConfig.put(DataExtensionOutputFormat.FAIL_ON_ERROR, String.valueOf(marketingCloudConf.shouldFailOnError()));
    outputConfig.put(DataExtensionOutputFormat.OPERATION, marketingCloudConf.getOperation().name());
    outputConfig.put(DataExtensionOutputFormat.DATA_EXTENSION_KEY, marketingCloudConf.getDataExtension());
    outputConfig.put(DataExtensionOutputFormat.TRUNCATE, String.valueOf(marketingCloudConf.shouldTruncateText()));
    Assert.assertEquals(outputConfig.size(), 9);
    outputFormatProvider.getOutputFormatConfiguration();


  }


}


