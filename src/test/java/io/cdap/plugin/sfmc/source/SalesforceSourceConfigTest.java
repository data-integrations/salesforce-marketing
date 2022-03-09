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

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import static io.cdap.plugin.sfmc.source.SalesforceSourceConfigHelper.TEST_REST_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_AUTH_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_CLIENT_ID;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_CLIENT_SECRET;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY_LIST;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_FILTER;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_OBJECT_LIST;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_OBJECT_NAME;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_QUERY_MODE;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_SOAP_API_ENDPOINT;
import static io.cdap.plugin.sfmc.source.util.MarketingCloudConstants.PROPERTY_TABLE_NAME_FIELD;

/**
 * Tests for {@link MarketingCloudSourceConfig}.
 */
public class SalesforceSourceConfigTest {

  protected static final String CLIENT_ID = "clientId";
  protected static final String CLIENT_SECRET = "clientSecret";
  protected static final String AUTH_ENDPOINT = "authEndPoint";
  protected static final String SOAP_ENDPOINT = "soapEndPoint";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testValidateReferenceName() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder().setReferenceName(
        "referenceName")
      .build();
    Assert.assertEquals("referenceName", config.getReferenceName());

  }

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
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_QUERY_MODE, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testRestApiEndPoint() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder().setRestEndpoint(
        "restApiEndpoint")
      .build();
    Assert.assertEquals("restApiEndpoint", config.getRestEndpoint());

  }

  @Test
  public void testTableNameField() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder().setTableNameField(
        "TableNameField")
      .build();
    Assert.assertEquals("TableNameField", config.getTableNameField());

  }

  @Test
  public void testFilter() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder().setFilter(
        "filter")
      .build();
    Assert.assertEquals("filter", config.getFilter());

  }

  @Test
  public void testDataExtensionKey() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder().setDataExtensionKey(
        "dataExtensionKey")
      .build();
    Assert.assertEquals("dataExtensionKey", config.getDataExtensionKey());

  }

  @Test
  public void testObjectListNull() {
    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder().setObjectList(null)
      .build();
    Assert.assertTrue(config.getObjectList().isEmpty());

  }


  @Test
  public void testValidateObjectNameNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setObjectName(null).build(), collector);
    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());

    }

  }

  @Test
  public void testInvalidObjectName() {

    MarketingCloudSourceConfig config = SalesforceSourceConfigHelper.newConfigBuilder()
      .setObjectName("").build();
    try {
      MockFailureCollector collector = new MockFailureCollector();
      config.getObject(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(PROPERTY_OBJECT_NAME, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(null, config.getObject());

    }
  }


  @Test
  public void testValidateClientIdNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Single Object")
      .setClientId(null)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
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
      .setClientId(CLIENT_ID)
      .setClientSecret(null)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
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
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(null)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_AUTH_API_ENDPOINT, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateRestEndPointNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Single Object")
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(null)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_API_ENDPOINT, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }




  @Test
  public void testValidateSoapEndpointNull() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Single Object")
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(null)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
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
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .setDataExtensionKey("test-dataextension-key")
      .build(), collector);

    config.validate(collector);

    Assert.assertEquals(0, collector.getValidationFailures().size());
    Assert.assertEquals(true, config.shouldConnect());

  }


  @Test
  public void testSingleObjectModeMissingDataExtensionKey() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Single Object")
      .setObjectName("Notsent event")
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .setDataExtensionKey(null)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_DATA_EXTENSION_KEY, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testMultiObjectModeMissingDataExtensionKeys() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Multi Object")
      .setObjectList("Notsent event,Data Extension")
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .setDataExtensionKeys(null)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(PROPERTY_DATA_EXTENSION_KEY_LIST, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(1, e.getFailures().size());

    }
  }

  @Test
  public void testMultiObjectModeMissingTableNameField() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Multi Object")
      .setObjectList("Notsent event,Data Extension")
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .setDataExtensionKeys("Test-DataExtension-Key1")
      .setTableNameField(null)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(PROPERTY_TABLE_NAME_FIELD, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(1, e.getFailures().size());
    }
  }

  @Test
  public void testMultiObjectModeMissingObject() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingCloudSourceConfig config = withSalesforceValidationMock(SalesforceSourceConfigHelper.newConfigBuilder()
      .setQueryMode("Multi Object")
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setRestEndpoint(TEST_REST_ENDPOINT)
      .setAuthEndpoint(AUTH_ENDPOINT)
      .setSoapEndpoint(SOAP_ENDPOINT)
      .setDataExtensionKeys("Test-DataExtension-Key1")
      .setTableNameField("tableName")
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(PROPERTY_OBJECT_LIST, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(1, e.getFailures().size());
    }
  }

  @Test
  public void testFilterWNull() throws ETSdkException {
    MockFailureCollector collector = new MockFailureCollector();
    try {
      MarketingCloudClient.validateFilter(null);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(PROPERTY_FILTER, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));

    }

  }


  private MarketingCloudSourceConfig withSalesforceValidationMock(MarketingCloudSourceConfig config,
                                                                  FailureCollector collector) {
    MarketingCloudSourceConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateSalesforceConnection(collector);
    return spy;
  }
}
