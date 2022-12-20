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

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DataExtensionClient.class)
public class MarketingCloudConfTest {
  public static final String TEST_REF_NAME = "TestRefName";
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String AUTH_ENDPOINT = "authEndPoint";
  private static final String SOAP_ENDPOINT = "soapEndPoint";
  private static final String TEST_DATA_EXTENSION = "DE";
  private static Schema schema;

  @Test
  public void testConfig() {
    SourceObject object = SourceObject.DATA_EXTENSION;
    MarketingCloudConf config = new MarketingCloudConf("referenceName", CLIENT_ID,
                                                       CLIENT_SECRET, "DE", AUTH_ENDPOINT,
                                                       SOAP_ENDPOINT, 500, null, "<key=value>",
                                                       true, true, true);
    Assert.assertEquals("referenceName", config.getReferenceName());
    Assert.assertEquals(500, config.getMaxBatchSize());
    Assert.assertTrue(config.shouldFailOnError());
    Assert.assertTrue(config.shouldReplaceWithSpaces());
    Assert.assertTrue(config.shouldTruncateText());
    Assert.assertEquals(Operation.INSERT, config.getOperation());
    Assert.assertEquals("Data Extension", object.getValue());
    Assert.assertEquals("Data Extension, Email, Mailing List, Bounce Event, Open Event, Unsub Event, Sent Event, " +
                          "Notsent Event", SourceObject.getSupportedObjects());
  }

  @Test
  public void testValidateReferenceName() {
    MarketingCloudConf config = MarketingCloudConfHelper.newConfigBuilder().setReferenceName(TEST_REF_NAME)
      .build();
    Assert.assertEquals("TestRefName", config.getReferenceName());
  }

  @Test
  public void testValidate() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudConf config = withSalesforceValidationMock(MarketingCloudConfHelper.newConfigBuilder()
                                                               .setClientId(CLIENT_ID)
                                                               .setClientSecret(CLIENT_SECRET)
                                                               .setAuthEndpoint(AUTH_ENDPOINT)
                                                               .setSoapEndpoint(SOAP_ENDPOINT)
                                                               .setDataExtension(TEST_DATA_EXTENSION)
                                                               .build(), collector);
    config.validate(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testMaxBatchSizeWhenProvidedNull() {
    MarketingCloudConf config = MarketingCloudConfHelper.newConfigBuilder()
      .setMaxBatchSize(null)
      .build();
    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(500, config.getMaxBatchSize());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBatchSizeInvalid() throws ETSdkException {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudConf config = withSalesforceValidationMock(MarketingCloudConfHelper.newConfigBuilder()
                                                               .setClientId(CLIENT_ID)
                                                               .setClientSecret(CLIENT_SECRET)
                                                               .setAuthEndpoint(AUTH_ENDPOINT)
                                                               .setSoapEndpoint(SOAP_ENDPOINT)
                                                               .setDataExtension(TEST_DATA_EXTENSION)
                                                               .setMaxBatchSize(-1)
                                                               .build(), collector);
    Schema inputSchema = Schema.recordOf("record",
                                         Schema.Field.of("store_id", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    try {
      config.validate(inputSchema, collector);
      collector.getOrThrowException();
      Assert.fail("Batch Size is invalid");
    } catch (ValidationException e) {
      // pass
    }
  }

  @Test
  public void testOperationNull() {
    MarketingCloudConf config = MarketingCloudConfHelper.newConfigBuilder()
      .setOperation(null)
      .build();
    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(Operation.INSERT, config.getOperation());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testOperationUpdate() {
    Operation operation = Operation.UPDATE;
    MarketingCloudConf config = MarketingCloudConfHelper.newConfigBuilder()
      .setOperation("UPDATE")
      .build();
    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(operation, config.getOperation());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetInvalidColumnMapping() {
    Schema inputSchema = Schema.recordOf("record",
                                         Schema.Field.of("storeid", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudConf config =
      MarketingCloudConfHelper.newConfigBuilder().setColumnMapping("columnMapping").build();
    try {
      config.getColumnMapping(inputSchema, collector);
      Assert.fail("Invalid ColumnMapping");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
    }
  }

  @Test
  public void testEmptyColumnMapping() {
    Schema inputSchema = Schema.recordOf("record",
                                         Schema.Field.of("store_id", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudConf config = new MarketingCloudConf("referenceName", CLIENT_ID,
                                                       CLIENT_SECRET, "DE", AUTH_ENDPOINT,
                                                       SOAP_ENDPOINT, 500, null, "column=mapping",
                                                       null, null, null);
    config.shouldFailOnError();
    config.shouldTruncateText();
    config.shouldReplaceWithSpaces();
    Assert.assertTrue(config.getColumnMapping(inputSchema, collector).isEmpty());
  }

  private MarketingCloudConf withSalesforceValidationMock(MarketingCloudConf config,
                                                          FailureCollector collector) {
    MarketingCloudConf spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validate(schema, collector);
    return spy;
  }
}
