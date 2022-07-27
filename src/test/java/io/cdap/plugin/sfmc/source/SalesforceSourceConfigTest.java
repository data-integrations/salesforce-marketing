/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.sfmc.connector.MarketingConnectorConfig;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static io.cdap.plugin.sfmc.source.SalesforceSourceConfigHelper.TEST_AUTH_ENDPOINT;
import static io.cdap.plugin.sfmc.source.SalesforceSourceConfigHelper.TEST_CLIENT_ID;
import static io.cdap.plugin.sfmc.source.SalesforceSourceConfigHelper.TEST_CLIENT_SECRET;
import static io.cdap.plugin.sfmc.source.SalesforceSourceConfigHelper.TEST_SOAP_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_AUTH_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_CLIENT_ID;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_CLIENT_SECRET;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY_LIST;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_OBJECT_NAME;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_QUERY_MODE;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_SOAP_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_TABLE_NAME_FIELD;


/**
 * Tests for {@link MarketingCloudSourceConfig}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MarketingConnectorConfig.class)
public class SalesforceSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testQueryModeSingleObject() {
    SourceQueryMode queryMode = SourceQueryMode.SINGLE_OBJECT;
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Single Object")
      .build();
    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(queryMode, config.getQueryMode(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testQueryModeMultiObject() {
    SourceQueryMode queryMode = SourceQueryMode.MULTI_OBJECT;
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Multi Object")
      .build();
    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(queryMode, config.getQueryMode(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testQueryModeInvalid() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode(null)
      .build();
    try {
      MockFailureCollector collector = new MockFailureCollector();
      config.getQueryMode(collector);
      Assert.fail("Query Mode is invalid");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_QUERY_MODE, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateClientIdNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Single Object")
                                                                       .setClientId(null)
                                                                       .setClientSecret(TEST_CLIENT_SECRET)
                                                                       .setAuthEndpoint(TEST_AUTH_ENDPOINT)
                                                                       .setSoapEndpoint(TEST_SOAP_ENDPOINT)
                                                                       .build(), collector);
    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Client Id is null");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_CLIENT_ID, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateClientSecretNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Single Object")
                                                                       .setClientId(TEST_CLIENT_ID)
                                                                       .setClientSecret(null)
                                                                       .setAuthEndpoint(TEST_AUTH_ENDPOINT)
                                                                       .setSoapEndpoint(TEST_SOAP_ENDPOINT)
                                                                       .build(), collector);
    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Client secret is null");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_CLIENT_SECRET, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateAuthEndpointNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Single Object")
                                                                       .setClientId(TEST_CLIENT_ID)
                                                                       .setClientSecret(TEST_CLIENT_SECRET)
                                                                       .setAuthEndpoint(null)
                                                                       .setSoapEndpoint(TEST_SOAP_ENDPOINT)
                                                                       .build(), collector);
    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("AuthEndPoint is null");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_AUTH_API_ENDPOINT, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateSoapEndpointNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Single Object")
                                                                       .setClientId(TEST_CLIENT_ID)
                                                                       .setClientSecret(TEST_CLIENT_SECRET)
                                                                       .setAuthEndpoint(TEST_AUTH_ENDPOINT)
                                                                       .setSoapEndpoint(null)
                                                                       .build(), collector);
    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("SoapEndPoint is null");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_SOAP_API_ENDPOINT, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  @Ignore
  public void testValidCredentials() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Single Object")
                                                                       .setClientId(TEST_CLIENT_ID)
                                                                       .setClientSecret(TEST_CLIENT_SECRET)
                                                                       .setAuthEndpoint(TEST_AUTH_ENDPOINT)
                                                                       .setSoapEndpoint(TEST_SOAP_ENDPOINT)
                                                                       .setDataExtensionKey("test-dataextension-key")
                                                                       .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testSingleObjectModeMissingDataExtensionKey() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Single Object")
                                                                       .setAuthEndpoint(TEST_AUTH_ENDPOINT)
                                                                       .setSoapEndpoint(TEST_SOAP_ENDPOINT)
                                                                       .setDataExtensionKey(null)
                                                                       .build(), collector);
    MarketingConnectorConfig connectorConfig = Mockito.mock(MarketingConnectorConfig.class);
    PowerMockito.whenNew(MarketingConnectorConfig.class).withArguments(Mockito.anyString(), Mockito.anyString(),
                                                                       Mockito.anyString(), Mockito.anyString())
                                                                       .thenReturn(connectorConfig);
    Mockito.when(config.getConnection()).thenReturn(connectorConfig);


    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("DtaExtension Key is missing");
    } catch (ValidationException e) {
      Assert.assertEquals(PROPERTY_DATA_EXTENSION_KEY, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testMultiObjectModeMissingDataExtensionKeys() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Multi Object")
                                                                       .setClientId(TEST_CLIENT_ID)
                                                                       .setClientSecret(TEST_CLIENT_SECRET)
                                                                       .setAuthEndpoint(TEST_AUTH_ENDPOINT)
                                                                       .setSoapEndpoint(TEST_SOAP_ENDPOINT)
                                                                       .setDataExtensionKeys(null)
                                                                       .build(), collector);
    MarketingConnectorConfig connectorConfig = Mockito.mock(MarketingConnectorConfig.class);
    PowerMockito.whenNew(MarketingConnectorConfig.class).withArguments(Mockito.anyString(), Mockito.anyString(),
                                                                       Mockito.anyString(), Mockito.anyString())
                                                                        .thenReturn(connectorConfig);
    Mockito.when(config.getConnection()).thenReturn(connectorConfig);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("DataExtension Keys are missing");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_DATA_EXTENSION_KEY_LIST, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testMultiObjectModeMissingTableNameField() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
                                                                       .setQueryMode("Multi Object")
                                                                       .setClientId(TEST_CLIENT_ID)
                                                                       .setClientSecret(TEST_CLIENT_SECRET)
                                                                       .setAuthEndpoint(TEST_AUTH_ENDPOINT)
                                                                       .setSoapEndpoint(TEST_SOAP_ENDPOINT)
                                                                       .setDataExtensionKeys("Test-DataExtension-Key1")
                                                                       .setTableNameField(null)
                                                                       .build(), collector);
    MarketingConnectorConfig connectorConfig = Mockito.mock(MarketingConnectorConfig.class);
    PowerMockito.whenNew(MarketingConnectorConfig.class).withArguments(Mockito.anyString(), Mockito.anyString(),
                                                                       Mockito.anyString(), Mockito.anyString())
                                                                        .thenReturn(connectorConfig);
    Mockito.when(config.getConnection()).thenReturn(connectorConfig);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Table name field is missing");
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_TABLE_NAME_FIELD, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testFilter() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder().setFilter(
        "filter")
      .build();
    Assert.assertEquals("filter", config.getFilter());
  }

  @Test
  public void testInvalidObjectName() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder()
      .setObjectName("").build();
    try {
      MockFailureCollector collector = new MockFailureCollector();
      config.getObject(collector);
      Assert.fail("Invalid Object");
    } catch (ValidationException e) {
      Assert.assertEquals(PROPERTY_OBJECT_NAME, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(null, config.getObject());
    }
  }

  private MarketingCloudSourceConfig withSalesforceValidationMock(MarketingCloudSourceConfig config,
                                                                  FailureCollector collector) {
    MarketingCloudSourceConfig spy = Mockito.spy(config);
    MarketingConnectorConfig connectorConfig = Mockito.mock(MarketingConnectorConfig.class);
    Mockito.doNothing().when(connectorConfig).validateSalesforceConnection(collector);
    return spy;
  }
}
