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
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.internal.EventType;
import io.cdap.cdap.api.data.format.StructuredRecord;
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
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
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
        MarketingCloudObjectInfo.class, Schema.Field.class})
public class MarketingConnectorTest {

  public SourceObject sourceObject;

  @Rule
  public ExpectedException thrown = ExpectedException.none();


  @Test
  public void testBrowse() {
    Assert.assertEquals(8, sourceObject.values().length);
  }

  @Test
  public void testConnector() throws IOException {
    MarketingConnectorConfig marketingConnectorConfig = Mockito.mock(MarketingConnectorConfig.class);
    MarketingConnector marketingConnector = new MarketingConnector(marketingConnectorConfig);
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    MarketingConnectorConfig connectorConfig = Mockito.mock(MarketingConnectorConfig.class);
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.when(connectorConfig.shouldConnect()).thenReturn(true);
    BrowseRequest browseRequest = Mockito.mock(BrowseRequest.class);
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    MarketingConnector connector = new MarketingConnector(connectorConfig);
    connector.test(context);
    connector.browse(context, browseRequest);
    PowerMockito.when(marketingConnectorConfig.getSchema(SourceObject.MAILING_LIST)).thenReturn(getPluginSchema());
    ConnectorSpec connectorSpec = marketingConnector.generateSpec
            (new MockConnectorContext(new MockConnectorConfigurer()),
            ConnectorSpecRequest.builder().setPath("MAILING_LIST").setConnection("${conn(connection-id)}").build());
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(2, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(MarketingCloudConstants.PLUGIN_NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());
    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
    Assert.assertEquals(0, collector.getValidationFailures().size());
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

  @Test
  public void testConvertToValueWRecord() {
    MarketingCloudUtil cloudConversion = new MarketingCloudUtil();
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
    Assert.assertNotNull(cloudConversion.convertToValue("store_id", fieldSchema, new HashMap<>
            (1)));
  }

  @Test
  public void testSample() throws IOException, ETSdkException, NoSuchFieldException {
    MarketingConnectorConfig connectorConfig = new MarketingConnectorConfig("clientId",
            "clientSecret", "authEndPoint", "soapEndPoint");
    MarketingConnector connector = new MarketingConnector(connectorConfig);
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
    Mockito.when(MarketingCloudClient.create(connectorConfig.getClientId(), connectorConfig.getClientSecret(),
            connectorConfig.getAuthEndpoint(), connectorConfig.getSoapEndpoint())).thenReturn(marketingCloudClient);
    Assert.assertNotEquals(SourceObject.DATA_EXTENSION, "MAILING_LIST");
    Mockito.doReturn(response).when(marketingCloudClient).fetchObjectRecords(SourceObject.MAILING_LIST, null);
    Mockito.doReturn(result).when(response).getObjects();
    FieldSetter.setField(connector, MarketingConnector.class.getDeclaredField("schema"), schema);
    Mockito.when(marketingCloudClient.fetchObjectSchema(SourceObject.MAILING_LIST)).thenReturn(objectInfo);
    Mockito.when(objectInfo.getSchema()).thenReturn(schema);
    Mockito.when(StructuredRecord.builder(schema)).thenReturn(recordBuilder);
    Mockito.when(iterator.next()).thenReturn(row);
    connector.sample(context, sampleRequest);
  }


  @Test
  public void testSchema() throws ETSdkException {
    MarketingConnectorConfig marketingConnectorConfig1 = new MarketingConnectorConfig
            ("clientId", "clientSecret", "authEndpoint", "soapEndpoint");
    MarketingCloudClient marketingCloudClient = Mockito.mock(MarketingCloudClient.class);
    MarketingCloudObjectInfo marketingCloudObjectInfo = Mockito.mock(MarketingCloudObjectInfo.class);
    PowerMockito.mockStatic(MarketingCloudObjectInfo.class);
    PowerMockito.mockStatic(MarketingCloudClient.class);
    Mockito.when(MarketingCloudClient.create("clientId", "clientSecret", "authEndpoint",
            "soapEndpoint")).thenReturn(marketingCloudClient);
    Mockito.when(MarketingCloudInputFormat.getTableMetaData
            (sourceObject, "Stores", marketingCloudClient)).thenReturn(marketingCloudObjectInfo);
    marketingConnectorConfig1.getSchema(sourceObject);
  }

  @Test
  public void testConvertToStringValue() {
    MarketingCloudUtil cloudUtil = new MarketingCloudUtil();
    Assert.assertEquals("Field Value", cloudUtil.convertToStringValue("Field Value"));
  }

  @Test
  public void testConvertToDoubleValue() {
    MarketingCloudUtil cloudUtil = new MarketingCloudUtil();
    Assert.assertEquals(42.0, cloudUtil.convertToDoubleValue("42").doubleValue(), 0.0);
    Assert.assertEquals(42.0, cloudUtil.convertToDoubleValue(42).doubleValue(), 0.0);
    Assert.assertNull(cloudUtil.convertToDoubleValue(""));
  }

  @Test
  public void testConvertToIntegerValue() {
    MarketingCloudUtil cloudUtil = new MarketingCloudUtil();
    Assert.assertEquals(42, cloudUtil.convertToIntegerValue("42").intValue());
    Assert.assertEquals(42, cloudUtil.convertToIntegerValue(42).intValue());
    Assert.assertNull(cloudUtil.convertToIntegerValue(""));
  }

  @Test
  public void testConvertToBooleanValue() {
    MarketingCloudUtil cloudUtil = new MarketingCloudUtil();
    Assert.assertFalse(cloudUtil.convertToBooleanValue("Field Value"));
    Assert.assertFalse(cloudUtil.convertToBooleanValue(42));
    Assert.assertNull(cloudUtil.convertToBooleanValue(""));
  }
}
