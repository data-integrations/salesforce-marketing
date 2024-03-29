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

import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataExtensionClient.class, ETClient.class, StructuredRecord.class, LineageRecorder.class})
public class MarketingCloudDataExtensionSinkTest {
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String AUTH_ENDPOINT = "authEndPoint";
  private static final String SOAP_ENDPOINT = "soapEndPoint";
  private MarketingCloudConf marketingCloudConf;
  private MarketingCloudDataExtensionSink marketingCloudDataExtensionSink;

  @Before
  public void initialize() throws ETSdkException {
    marketingCloudConf = Mockito.spy(MarketingCloudConfHelper.newConfigBuilder().
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
                                       .setReplaceWithSpaces(true)
                                       .build());
    marketingCloudDataExtensionSink = new MarketingCloudDataExtensionSink(marketingCloudConf);
    Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, AUTH_ENDPOINT, SOAP_ENDPOINT);
  }

  @Test
  public void testConfigurePipelineWithMapReduce() {
    MarketingCloudConf cloudConf = Mockito.mock(MarketingCloudConf.class);
    MarketingCloudDataExtensionSink dataExtensionSink =
      new MarketingCloudDataExtensionSink(cloudConf);
    BatchRuntimeContext context = Mockito.mock(BatchRuntimeContext.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    StageConfigurer stageConfigurer = PowerMockito.mock(StageConfigurer.class);
    FailureCollector failureCollector = PowerMockito.mock(FailureCollector.class);
    PipelineConfigurer pipelineConfigurer = PowerMockito.mock(PipelineConfigurer.class);
    PowerMockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(stageConfigurer);
    PowerMockito.when(stageConfigurer.getFailureCollector()).thenReturn(failureCollector);
    PowerMockito.when(cloudConf.shouldFailOnError()).thenReturn(Boolean.TRUE);
    Mockito.when(pipelineConfigurer.getEngine()).thenReturn(Engine.MAPREDUCE);
    dataExtensionSink.initialize(context);
    dataExtensionSink.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testConfigurePipelineWithSpark() {
    MarketingCloudConf cloudConf = Mockito.mock(MarketingCloudConf.class);
    MarketingCloudDataExtensionSink dataExtensionSink =
      new MarketingCloudDataExtensionSink(cloudConf);
    BatchRuntimeContext context = Mockito.mock(BatchRuntimeContext.class);
    Map<String, Object> plugins = new HashMap<>();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    StageConfigurer stageConfigurer = PowerMockito.mock(StageConfigurer.class);
    FailureCollector failureCollector = PowerMockito.mock(FailureCollector.class);
    PipelineConfigurer pipelineConfigurer = PowerMockito.mock(PipelineConfigurer.class);
    PowerMockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(stageConfigurer);
    PowerMockito.when(stageConfigurer.getFailureCollector()).thenReturn(failureCollector);
    PowerMockito.when(cloudConf.shouldFailOnError()).thenReturn(Boolean.TRUE);
    Mockito.when(pipelineConfigurer.getEngine()).thenReturn(Engine.SPARK);
    dataExtensionSink.initialize(context);
    dataExtensionSink.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testConfigurePipelineWithInvalidColumnMapping() {
    BatchRuntimeContext context = Mockito.mock(BatchRuntimeContext.class);
    Map<String, Object> plugins = new HashMap<>();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    try {
      marketingCloudDataExtensionSink.initialize(context);
      marketingCloudDataExtensionSink.configurePipeline(mockPipelineConfigurer);
      mockFailureCollector.getOrThrowException();
      Assert.fail("Exception is thrown for invalid column mapping");
    } catch (ValidationException e) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
      Assert.assertEquals("Errors were encountered during validation. Invalid column name: <key",
                          e.getMessage());
    }
  }

  @Test
  public void testPrepareRunWithInvalidColumnMapping() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSinkContext context = Mockito.mock(BatchSinkContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(mockArguments);
    try {
      marketingCloudDataExtensionSink.prepareRun(context);
      mockFailureCollector.getOrThrowException();
      Assert.fail("Exception is thrown for invalid column mapping");
    } catch (ValidationException e) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
      Assert.assertEquals("Errors were encountered during validation. Invalid column name: <key",
                          e.getMessage());
    }
  }

  @Test
  public void testPrepareRunWithInvalidClient() {
    Schema inputSchema = Schema.recordOf("record",
                                         Schema.Field.of("storeid", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MockArguments mockArguments = new MockArguments();
    BatchSinkContext batchSinkContext = Mockito.mock(BatchSinkContext.class);
    Mockito.when(batchSinkContext.getInputSchema()).thenReturn(inputSchema);
    Mockito.when(batchSinkContext.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(batchSinkContext.getArguments()).thenReturn(mockArguments);
    Map<String, String> columnMapping = new HashMap<>();
    Mockito.doReturn(columnMapping).when(marketingCloudConf).getColumnMapping(inputSchema, mockFailureCollector);
    try {
      marketingCloudDataExtensionSink.prepareRun(batchSinkContext);
      mockFailureCollector.getOrThrowException();
      Assert.fail("Exception is thrown for empty client_id");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("Errors were encountered during validation. Error while validating Marketing " +
                            "Cloud client: authEndPoint/v2/token: bad URL", e.getMessage());
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

  @Test
  public void testGetOutputFormatConfiguration() {
    OutputFormatProvider outputFormatProvider = Mockito.mock(OutputFormatProvider.class);
    Map<String, String> outputConfig = new HashMap<>();
    outputConfig.put(DataExtensionOutputFormat.CLIENT_ID, marketingCloudConf.getConnection().getClientId());
    outputConfig.put(DataExtensionOutputFormat.CLIENT_SECRET, marketingCloudConf.getConnection().getClientSecret());
    outputConfig.put(DataExtensionOutputFormat.AUTH_ENDPOINT, marketingCloudConf.getConnection().getAuthEndpoint());
    outputConfig.put(DataExtensionOutputFormat.SOAP_ENDPOINT, marketingCloudConf.getConnection().getSoapEndpoint());
    outputConfig.put(DataExtensionOutputFormat.MAX_BATCH_SIZE, String.valueOf(marketingCloudConf.getMaxBatchSize()));
    outputConfig.put(DataExtensionOutputFormat.FAIL_ON_ERROR, String.valueOf(marketingCloudConf.shouldFailOnError()));
    outputConfig.put(DataExtensionOutputFormat.OPERATION, marketingCloudConf.getOperation().name());
    outputConfig.put(DataExtensionOutputFormat.DATA_EXTENSION_KEY, marketingCloudConf.getDataExtension());
    outputConfig.put(DataExtensionOutputFormat.TRUNCATE, String.valueOf(marketingCloudConf.shouldTruncateText()));
    outputFormatProvider.getOutputFormatConfiguration();
    Assert.assertEquals(outputConfig.size(), 9);
  }

  @Test
  public void testTransform() {
    MarketingCloudConf conf = new MarketingCloudConf("referenceName", "clientId",
      "clientSecret", "dataExtension", "authEndPoint", "soapEndPoint",
      3, "operation", null, true,
      true, true);
    MarketingCloudDataExtensionSink marketingCloudDataExtensionSink1 = new MarketingCloudDataExtensionSink(conf);
    StructuredRecord input = Mockito.mock(StructuredRecord.class);
    Emitter<KeyValue<NullWritable, StructuredRecord>> emitter = Mockito.mock(Emitter.class);
    Schema inputSchema = Schema.recordOf("record",
                                         Schema.Field.of("storeid", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    MockFailureCollector collector = new MockFailureCollector("SFMCSink");
    Assert.assertTrue(conf.getColumnMapping(inputSchema, collector).isEmpty());
    try {
      marketingCloudDataExtensionSink1.transform(input, emitter);
    } catch (Exception e) {
    }
  }

  @Test
  public void testTransformColumnMappingNotEmpty() {
    List<Schema.Field> getFields = new ArrayList<>();
    BatchRuntimeContext context = Mockito.mock(BatchRuntimeContext.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    MarketingCloudDataExtensionSink marketingCloudDataExtensionSink1 =
      new MarketingCloudDataExtensionSink(marketingCloudConf);
    StructuredRecord input = Mockito.mock(StructuredRecord.class);
    Emitter<KeyValue<NullWritable, StructuredRecord>> emitter = Mockito.mock(Emitter.class);
    Schema inputSchema = Schema.recordOf("record",
                                         Schema.Field.of("store_id", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("email_id", Schema.of(Schema.Type.STRING)));
    getFields.add(inputSchema.getField("store_id"));
    getFields.add(inputSchema.getField("email_id"));
    MockFailureCollector collector = new MockFailureCollector("SFMCSink");
    Mockito.when(context.getInputSchema()).thenReturn(inputSchema);
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    try {
      marketingCloudDataExtensionSink1.initialize(context);
      Mockito.when(input.getSchema()).thenReturn(inputSchema);
      marketingCloudDataExtensionSink1.transform(input, emitter);
    } catch (Exception e) {
    }
  }
}

