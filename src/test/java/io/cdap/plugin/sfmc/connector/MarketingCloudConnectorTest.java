/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.sfmc.connector;

import com.custom.fuelsdk.ETClickEvent;
import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETDataExtension;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.internal.EventType;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.sfmc.source.MarketingCloudClient;
import io.cdap.plugin.sfmc.source.MarketingCloudInputFormat;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.MarketingCloudUtil;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MarketingCloudClient.class, MarketingCloudInputFormat.class, StructuredRecord.class,
  MarketingCloudObjectInfo.class, Schema.Field.class, ETDataExtension.class, Schema.class, ETClient.class,
  MarketingCloudUtil.class})
public class MarketingCloudConnectorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConvertToValueWRecord() {
    Schema fieldSchema = Schema.recordOf("record",
                                         Schema.Field.of("store_id", Schema.of(Schema.Type.DOUBLE)),
                                         Schema.Field.of("markedPrice", Schema.nullableOf(Schema.decimalOf
                                           (4, 2))),
                                         Schema.Field.of("timestamp",
                                                         Schema.nullableOf(Schema.of
                                                           (Schema.LogicalType.TIMESTAMP_MICROS))),
                                         Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                         Schema.Field.of("bytes", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.DOUBLE)));
    thrown.expect(IllegalStateException.class);
    Assert.assertNotNull(MarketingCloudUtil.convertToValue("store_id", fieldSchema, new HashMap<>
      (1)));
  }

  @Test
  public void testConvertToValueWRecordTimeStamp() {
    Schema fieldSchema = Schema.recordOf("record",
                                         Schema.Field.of("store_id", Schema.of(Schema.Type.INT)),
                                         Schema.Field.of("markedPrice", Schema.nullableOf(Schema.decimalOf
                                           (4, 2))),
                                         Schema.Field.of("timestamp",
                                                         Schema.nullableOf(Schema.of
                                                           (Schema.LogicalType.DATE))),
                                         Schema.Field.of("price", Schema.of(Schema.Type.INT)),
                                         Schema.Field.of("bytes", Schema.of(Schema.LogicalType.DATE)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.INT)));
    thrown.expect(IllegalStateException.class);
    Assert.assertNotNull(MarketingCloudUtil.convertToValue("store_id", fieldSchema, new HashMap<>
      (1)));
  }

  @Test
  public void testConvertToStringValue() {
    Assert.assertEquals("Field Value", MarketingCloudUtil.convertToStringValue("Field Value"));
  }

  @Test
  public void testConvertToDoubleValue() {
    Assert.assertEquals(42.0, MarketingCloudUtil.convertToDoubleValue("42").doubleValue(), 0.0);
    Assert.assertEquals(42.0, MarketingCloudUtil.convertToDoubleValue(42).doubleValue(), 0.0);
    Assert.assertNull(MarketingCloudUtil.convertToDoubleValue(""));
  }

  @Test
  public void testConvertToIntegerValue() {
    Assert.assertEquals(42, MarketingCloudUtil.convertToIntegerValue("42").intValue());
    Assert.assertEquals(42, MarketingCloudUtil.convertToIntegerValue(42).intValue());
    Assert.assertNull(MarketingCloudUtil.convertToIntegerValue(""));
  }

  @Test
  public void testConvertToBooleanValue() {
    Assert.assertFalse(MarketingCloudUtil.convertToBooleanValue("Field Value"));
    Assert.assertFalse(MarketingCloudUtil.convertToBooleanValue(42));
    Assert.assertNull(MarketingCloudUtil.convertToBooleanValue(""));
  }

  private Schema getPluginSchema() throws IOException {
    String schemaString = "{\"type\":\"record\",\"name\":\"SalesforceMarketingCloud\",\"fields\":[{\"name\":" +
      "\"backgroundElementId\",\"type\":\"long\"},{\"name\":\"bgOrderPos\",\"type\":\"long\"},{\"name\":" +
      "\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"endDate\",\"type\":[{\"type\":\"long\"," +
      "\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"lastModifiedDate\",\"type\":" +
      "[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"project\",\"type\":" +
      "\"string\"},{\"name\":\"startDate\",\"type\":[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}," +
      "\"null\"]},{\"name\":\"userId\",\"type\":\"string\"}]}";
    return Schema.parseJson(schemaString);
  }

  /**
   * Expected Validation Exception i.e. 'Unable to connect to Salesforce Instance'.
   */
  @Test
  public void testBrowseFunctionalityWException() {
    MarketingConnectorConfig connectorConfig = new MarketingConnectorConfig("clientId",
                                                                            "clientSecret", "authEndPoint",
                                                                            "soapEndPoint");
    BrowseRequest browseRequest = Mockito.mock(BrowseRequest.class);
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    MarketingCloudConnector connector = new MarketingCloudConnector(connectorConfig);
    Mockito.when(browseRequest.getPath()).thenReturn(SourceObject.MAILING_LIST.toString());
    try {
      connector.browse(context, browseRequest);
    } catch (Exception e) {
      Assert.assertEquals(84, e.getMessage().length());
    }
  }

  @Test
  public void testConnector() throws IOException, ETSdkException {
    MarketingConnectorConfig connectorConfig = Mockito.mock(MarketingConnectorConfig.class);
    new MarketingConnectorConfig("clientID", "clientSecret", "authPoint",
                                 "soapEndPoint");
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.when(connectorConfig.shouldConnect()).thenReturn(true);
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    MarketingCloudConnector connector = new MarketingCloudConnector(connectorConfig);
    connector.test(context);
    PowerMockito.mockStatic(MarketingCloudClient.class);
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    PowerMockito.when(MarketingCloudClient.getOrCreate(connectorConfig.getClientId(), connectorConfig.getClientSecret(),
                                                       connectorConfig.getAuthEndpoint(),
                                                       connectorConfig.getSoapEndpoint())).
      thenReturn(marketingCloudClient);
    PowerMockito.mockStatic(MarketingCloudClient.class);
    PowerMockito.when(connectorConfig.getSchema(SourceObject.MAILING_LIST)).thenReturn(getPluginSchema());
    ConnectorSpec connectorSpec = connector.generateSpec(new MockConnectorContext(new MockConnectorConfigurer()),
                                                         ConnectorSpecRequest.builder().setPath
                                                             (SourceObject.MAILING_LIST.toString())
                                                           .setConnection("${conn(connection-id)}").build());
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(2, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());
    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testConnectorDataExtension() throws IOException, ETSdkException {
    MarketingConnectorConfig connectorConfig = new MarketingConnectorConfig("clientId", "clientSecret",
                                                                            "authEndPoint",
                                                                            "soapEndPoint");
    MockFailureCollector collector = new MockFailureCollector();
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    MarketingCloudConnector connector = new MarketingCloudConnector(connectorConfig);
    PowerMockito.mockStatic(MarketingCloudClient.class);
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    PowerMockito.mockStatic(MarketingCloudUtil.class);
    MarketingCloudUtil marketingCloudUtil = Mockito.mock(MarketingCloudUtil.class);
    Mockito.when(MarketingCloudClient.getOrCreate("clientId", "clientSecret",
                                                  "authEndPoint", "soapEndPoint")).
      thenReturn(marketingCloudClient);
    MarketingCloudObjectInfo marketingCloudObjectInfo = Mockito.mock(MarketingCloudObjectInfo.class);
    Mockito.when(marketingCloudClient.fetchDataExtensionSchema("DATA_EXTENSION")).
      thenReturn(marketingCloudObjectInfo);
    Mockito.when(marketingCloudClient.fetchDataExtensionSchema("DATA_EXTENSION").getSchema()).
      thenReturn(getPluginSchema());
    ConnectorSpec connectorSpec = connector.generateSpec(new MockConnectorContext(new MockConnectorConfigurer()),
                                                         ConnectorSpecRequest.builder().setPath
                                                             (SourceObject.DATA_EXTENSION.toString())
                                                           .setConnection("${conn(connection-id)}").build());
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(2, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testSample() throws IOException, ETSdkException, NoSuchFieldException {
    MarketingConnectorConfig connectorConfig = new MarketingConnectorConfig("clientId", "clientSecret",
                                                                            "authEndPoint",
                                                                            "soapEndPoint");
    MarketingCloudConnector connector = new MarketingCloudConnector(connectorConfig);
    SampleRequest sampleRequest = Mockito.mock(SampleRequest.class);
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    MarketingCloudObjectInfo objectInfo = Mockito.mock(MarketingCloudObjectInfo.class);
    Mockito.when(sampleRequest.getPath()).thenReturn("MAILING_LIST");
    ETResponse<? extends ETSoapObject> response = Mockito.spy(ETResponse.class);
    List<Schema.Field> tableFields = Mockito.spy(new ArrayList<>());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
    Schema.Field field = schema.getField("id", true);
    Mockito.mock(Schema.Field.class);
    PowerMockito.mockStatic(Schema.Field.class);
    field.getName();
    field.getSchema();
    field.toString();
    tableFields.add(field);
    Iterator iterator = Mockito.spy(Iterator.class);
    List<? extends ETApiObject> result = new ArrayList<>();
    List<StructuredRecord> sample = new ArrayList<>();
    List<ETClickEvent> list = new ArrayList<>();
    ETApiObject row = new ETDataExtensionRow();
    row.setId("id");
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setSendID(121);
    etClickEvent.setEventType(EventType.CLICK);
    list.add(etClickEvent);
    result = list;
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    PowerMockito.spy(StructuredRecord.class);
    StructuredRecord.Builder recordBuilder = PowerMockito.spy(StructuredRecord.builder(schema));
    PowerMockito.when(StructuredRecord.builder(schema)).thenReturn(recordBuilder);
    PowerMockito.mockStatic(MarketingCloudClient.class);
    Mockito.when(MarketingCloudClient.getOrCreate(connectorConfig.getClientId(), connectorConfig.getClientSecret(),
                                                  connectorConfig.getAuthEndpoint(), connectorConfig.getSoapEndpoint()))
      .thenReturn(marketingCloudClient);
    Mockito.doReturn(response).when(marketingCloudClient).fetchObjectRecords(SourceObject.MAILING_LIST, null,
      null);
    Mockito.doReturn(result).when(response).getObjects();
    FieldSetter.setField(connector, MarketingCloudConnector.class.getDeclaredField("schema"), schema);
    Mockito.when(marketingCloudClient.fetchObjectSchema(SourceObject.MAILING_LIST)).thenReturn(objectInfo);
    Mockito.when(objectInfo.getSchema()).thenReturn(schema);
    Mockito.when(StructuredRecord.builder(schema)).thenReturn(recordBuilder);
    Mockito.when(iterator.next()).thenReturn(row);
    connector.sample(context, sampleRequest);
  }


  @Test
  public void testBrowseWMailingList() throws ETSdkException {
    MockFailureCollector collector = Mockito.mock(MockFailureCollector.class);
    MarketingConnectorConfig marketingConnectorConfig = new MarketingConnectorConfig("clientId",
                                                                                     "clientSecret",
                                                                                     "authEndPoint",
                                                                                     "soapEndPoint");
    MarketingCloudConnector marketingCloudConnector = new MarketingCloudConnector(marketingConnectorConfig);
    BrowseRequest browseRequest = Mockito.mock(BrowseRequest.class);
    Mockito.when(browseRequest.getPath()).thenReturn("MAILING_LIST");
    PowerMockito.mockStatic(MarketingCloudClient.class);
    ConnectorContext context = Mockito.mock(ConnectorContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(collector);
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    PowerMockito.when(MarketingCloudClient.getOrCreate("clientId",
                                                       "clientSecret",
                                                       "authEndPoint",
                                                       "soapEndPoint")).thenReturn(marketingCloudClient);
    marketingCloudConnector.browse(context, browseRequest);
  }

  @Test
  public void testBrowseWDATAEXTENSION() throws ETSdkException {
    MockFailureCollector collector = Mockito.mock(MockFailureCollector.class);
    MarketingConnectorConfig marketingConnectorConfig = new MarketingConnectorConfig("clientId",
                                                                                     "clientSecret",
                                                                                     "authEndPoint",
                                                                                     "soapEndPoint");
    MarketingCloudConnector marketingCloudConnector = new MarketingCloudConnector(marketingConnectorConfig);
    BrowseRequest browseRequest = Mockito.mock(BrowseRequest.class);
    Mockito.when(browseRequest.getPath()).thenReturn("DATA_EXTENSION");
    ETResponse etResponse = Mockito.mock(ETResponse.class);
    PowerMockito.mockStatic(MarketingCloudClient.class);
    ConnectorContext context = Mockito.mock(ConnectorContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(collector);
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    PowerMockito.when(MarketingCloudClient.getOrCreate("clientId",
                                                       "clientSecret",
                                                       "authEndPoint",
                                                       "soapEndPoint")).thenReturn(marketingCloudClient);
    Mockito.when(marketingCloudClient.retrieveDataExtensionKeys()).thenReturn(etResponse);
    marketingCloudConnector.browse(context, browseRequest);
  }

  @Test(expected = UnexpectedFormatException.class)
  public void testDataExtensionList() throws ETSdkException, IOException, NoSuchFieldException {
    MarketingConnectorConfig connectorConfig = new MarketingConnectorConfig("clientId", "clientSecret",
                                                                            "authEndPoint", "soapEndPoint");
    MarketingCloudConnector marketingCloudConnector = new MarketingCloudConnector(connectorConfig);
    SampleRequest sampleRequest = Mockito.mock(SampleRequest.class);
    ConnectorContext context = Mockito.mock(ConnectorContext.class);
    MarketingCloudObjectInfo objectInfo = Mockito.mock(MarketingCloudObjectInfo.class);
    Mockito.when(sampleRequest.getPath()).thenReturn("DATA_EXTENSION/DE");
    ETResponse<ETDataExtensionRow> response1 = Mockito.mock(ETResponse.class);
    List<Schema.Field> tableFields = Mockito.spy(new ArrayList<>());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
    Schema.Field field = schema.getField("id", true);
    Mockito.mock(Schema.Field.class);
    PowerMockito.mockStatic(Schema.Field.class);
    field.getName();
    field.getSchema();
    field.toString();
    tableFields.add(field);
    Iterator iterator = Mockito.spy(Iterator.class);
    List<? extends ETApiObject> result = new ArrayList<>();
    List<StructuredRecord> sample = new ArrayList<>();
    List<ETClickEvent> list = new ArrayList<>();
    ETApiObject row = new ETDataExtensionRow();
    row.setId("id");
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setSendID(121);
    etClickEvent.setEventType(EventType.CLICK);
    list.add(etClickEvent);
    result = list;
    PowerMockito.mockStatic(ETClient.class);
    ETClient etClient = Mockito.mock(ETClient.class);
    PowerMockito.mockStatic(MarketingCloudUtil.class);
    MarketingCloudUtil marketingCloudUtil = Mockito.mock(MarketingCloudUtil.class);
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    PowerMockito.spy(StructuredRecord.class);
    StructuredRecord.Builder recordBuilder = PowerMockito.spy(StructuredRecord.builder(schema));
    PowerMockito.mockStatic(MarketingCloudClient.class);
    Mockito.when(MarketingCloudClient.getOrCreate(connectorConfig.getClientId(), connectorConfig.getClientSecret(),
                                                  connectorConfig.getAuthEndpoint(), connectorConfig.getSoapEndpoint()))
      .thenReturn(marketingCloudClient);
    Mockito.when(marketingCloudClient.fetchDataExtensionRecords(Mockito.anyString(),
                                                                Mockito.any(), Mockito.any())).thenReturn(response1);
    Mockito.doReturn(result).when(response1).getObjects();
    FieldSetter.setField(marketingCloudConnector, MarketingCloudConnector.class.getDeclaredField("schema"), schema);
    Mockito.when(marketingCloudClient.fetchDataExtensionSchema(Mockito.anyString())).thenReturn(objectInfo);
    Mockito.when(objectInfo.getSchema()).thenReturn(schema);
    Mockito.when(iterator.hasNext()).thenReturn(Boolean.TRUE);
    Mockito.when(StructuredRecord.builder(schema)).thenReturn(recordBuilder);
    Mockito.when(iterator.next()).thenReturn(row);
    marketingCloudConnector.sample(context, sampleRequest);
  }

  /**
   * throwing IO exception
   */
  @Test
  public void testDataExtensionListWException() {
    MockFailureCollector collector = new MockFailureCollector();
    MarketingConnectorConfig connectorConfig = new MarketingConnectorConfig("clientId",
                                                                            "clientSecret",
                                                                            "authEndPoint",
                                                                            "soapEndPoint");
    MarketingCloudConnector connector = new MarketingCloudConnector(connectorConfig);
    SampleRequest sampleRequest = Mockito.mock(SampleRequest.class);
    ConnectorContext context = Mockito.mock(ConnectorContext.class);
    Mockito.when(sampleRequest.getPath()).thenReturn(null);
    Assert.assertNotEquals(SourceObject.TRACKING_UNSUB_EVENT, null);
    try {
      collector.getOrThrowException();
      Assert.assertEquals(connector.sample(context, sampleRequest), true);
    } catch (IOException e) {
      Assert.assertEquals(e.getMessage(), "unable to fetch records");
    }
  }
}
