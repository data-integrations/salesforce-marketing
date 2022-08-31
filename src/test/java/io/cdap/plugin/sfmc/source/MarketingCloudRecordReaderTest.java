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

import com.custom.fuelsdk.ETClickEvent;
import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETConfiguration;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSoapConnection;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.internal.EventType;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ETClient.class, ClassLoader.class, MarketingCloudClient.class, ETSoapConnection.class,
  StructuredRecord.class, Schema.Field.class, Schema.class})
public class MarketingCloudRecordReaderTest {
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String AUTH_ENDPOINT = "authEndPoint";
  private static final String SOAP_ENDPOINT = "soapEndPoint";
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudInputFormat.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private MarketingCloudSourceConfig marketingCloudSourceConfig;
  private MarketingCloudRecordReader marketingCloudRecordReader;

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
    marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
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
    marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
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
      marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
  }

  @Test
  public void testConstructor2() throws IOException {

    MarketingCloudSourceConfig marketingCloudSourceConfig = new MarketingCloudSourceConfig("referenceName",
                                                                                           "Single Object",
                                                                                           "Data Extension",
                                                                                           "DE",
                                                                                           "objectList",
                                                                                           null,
                                                                                           "tableNameField",
                                                                                           "filter", CLIENT_ID,
                                                                                           CLIENT_SECRET,
                                                                                           AUTH_ENDPOINT,
                                                                                           SOAP_ENDPOINT);
    Assert.assertEquals("DE", marketingCloudSourceConfig.getDataExtensionKey());
    Assert.assertEquals(SourceQueryMode.SINGLE_OBJECT, marketingCloudSourceConfig.getQueryMode());
    Assert.assertEquals(null, marketingCloudSourceConfig.getDataExtensionKeys());
    Assert.assertEquals("tableNameField", marketingCloudSourceConfig.getTableNameField());
    Assert.assertEquals(SourceObject.DATA_EXTENSION, marketingCloudSourceConfig.getObject());
    Assert.assertEquals("referenceName", marketingCloudSourceConfig.getReferenceName());
    PluginProperties properties = marketingCloudSourceConfig.getProperties();
    Assert.assertTrue(properties.getProperties().isEmpty());
  }

  @Test
  public void testFetchData() throws Exception {
    MarketingCloudInputSplit split = new MarketingCloudInputSplit("TRACKING_UNSUB_EVENT", "unsub");
    SourceObject object = SourceObject.valueOf(split.getObjectName());
    MarketingCloudRecordReader marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
    ETResponse<? extends ETSoapObject> response = Mockito.spy(new ETResponse<>());
    response.setRequestId("id");
    response.setResponseMessage("MoreDataAvailable");
    List<? extends ETSoapObject> results = new ArrayList<>();
    List<ETClickEvent> list = new ArrayList<>();
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setSendID(121);
    etClickEvent.setEventType(EventType.CLICK);
    list.add(etClickEvent);
    results = list;
    ETConfiguration conf = Mockito.spy(new ETConfiguration());
    conf.set("clientId", CLIENT_ID);
    conf.set("clientSecret", CLIENT_SECRET);
    conf.set("authEndpoint", AUTH_ENDPOINT);
    conf.set("soapEndpoint", SOAP_ENDPOINT);
    conf.set("useOAuth2Authentication", "true");
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column = new MarketingCloudColumn("price", "String");
    columns.add(column);
    MarketingCloudObjectInfo sObjectInfo = Mockito.spy(new MarketingCloudObjectInfo(object, columns));
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.spy(new MarketingCloudClient(new ETClient(conf)));
    MarketingCloudClient client = PowerMockito.mock(MarketingCloudClient.class);
    PowerMockito.whenNew(MarketingCloudClient.class).withArguments(Mockito.any()).thenReturn(client);
    PowerMockito.mockStatic(ClassLoader.class);
    Mockito.when(client.fetchObjectSchema(object)).thenReturn(sObjectInfo);
    Mockito.when(sObjectInfo.getSchema()).thenReturn(schema);
    Mockito.doReturn(response).when(client).fetchObjectRecords(object, null);
    Mockito.doReturn(results).when(response).getObjects();
    marketingCloudRecordReader.initialize(split, null);
    Assert.assertTrue(marketingCloudRecordReader.nextKeyValue());
  }

  @Test
  public void testFetchDataWithDataExtension() throws Exception {
    MarketingCloudInputSplit split = new MarketingCloudInputSplit("DATA_EXTENSION", "dataextension");
    ETResponse<ETDataExtensionRow> responses = Mockito.spy(new ETResponse<>());
    responses.setRequestId("id");
    responses.setResponseMessage("");
    SourceObject object = SourceObject.valueOf(split.getObjectName());
    object.setFilter("filter");
    MarketingCloudRecordReader marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
    List<? extends ETSoapObject> results = Mockito.spy(new ArrayList<>());
    List<ETClickEvent> list = new ArrayList<>();
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setSendID(121);
    etClickEvent.setEventType(EventType.CLICK);
    list.add(etClickEvent);
    ETConfiguration conf = Mockito.spy(new ETConfiguration());
    conf.set("clientId", CLIENT_ID);
    conf.set("clientSecret", CLIENT_SECRET);
    conf.set("authEndpoint", AUTH_ENDPOINT);
    conf.set("soapEndpoint", SOAP_ENDPOINT);
    conf.set("useOAuth2Authentication", "true");
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column = new MarketingCloudColumn("price", "String");
    columns.add(column);
    MarketingCloudObjectInfo sObjectInfo = Mockito.spy(new MarketingCloudObjectInfo(object, columns));
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.spy(new MarketingCloudClient(new ETClient(conf)));
    MarketingCloudClient client = PowerMockito.mock(MarketingCloudClient.class);
    PowerMockito.whenNew(MarketingCloudClient.class).withArguments(Mockito.any()).thenReturn(client);
    PowerMockito.mockStatic(ClassLoader.class);
    Mockito.when(client.fetchObjectSchema(object)).thenReturn(sObjectInfo);
    Mockito.when(sObjectInfo.getSchema()).thenReturn(schema);
    Mockito.when(client.fetchDataExtensionRecords(
      SourceObject.DATA_EXTENSION.getTableName(), "filter", null)).thenReturn(responses);
    Mockito.when(responses.getObjects()).thenReturn((List<ETDataExtensionRow>) results);
    marketingCloudRecordReader.initialize(split, null);
    Assert.assertFalse(marketingCloudRecordReader.nextKeyValue());
  }

  @Test
  public void testGetCurrentKey() throws IOException, InterruptedException {
    MarketingCloudRecordReader marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
    Assert.assertNotNull(marketingCloudRecordReader.getCurrentKey());
  }

  @Test
  public void testConvertToValueBooleanType() {
    Schema fieldSchema = Schema.of(Schema.Type.BOOLEAN);
    Assert.assertNotNull(marketingCloudRecordReader.convertToValue("Field Name", fieldSchema,
                                                                   new HashMap<>(1)));
  }

  @Test
  public void testConvertToValueStringFieldType() {
    Schema fieldSchema = Schema.of(Schema.Type.STRING);
    Assert.assertNotNull(marketingCloudRecordReader.convertToValue("Field Name", fieldSchema,
                                                                   new HashMap<>(1)));
  }

  @Test
  public void testGetCurrentKeyValue() throws IOException, NoSuchFieldException {
    ETApiObject row = new ETDataExtensionRow();
    row.setId("id");
    List<MarketingCloudColumn> columns = new ArrayList<>();
    MarketingCloudColumn column = new MarketingCloudColumn("name", "String");
    columns.add(column);
    MarketingCloudObjectInfo sfObjectMetaData =
      new MarketingCloudObjectInfo(SourceObject.TRACKING_UNSUB_EVENT, columns);
    List<Schema.Field> list = Mockito.spy(new ArrayList<>());
    String tableNameField = "store_id";
    String formattedTableName = "storeid";
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("store_id", Schema.nullableOf(Schema.decimalOf(4, 2))),
                                    Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    PowerMockito.spy(StructuredRecord.class);
    StructuredRecord.Builder recordBuilder = PowerMockito.spy(StructuredRecord.builder(schema));
    PowerMockito.when(StructuredRecord.builder(schema)).thenReturn(recordBuilder);
    recordBuilder.set("store_id", 121);
    recordBuilder.set("emailid", "abc@123");
    Mockito.when(recordBuilder.set(tableNameField, formattedTableName)).thenReturn(recordBuilder);
    FieldSetter.setField(marketingCloudRecordReader, MarketingCloudRecordReader.class.getDeclaredField("schema"),
                         schema);
    FieldSetter.setField(marketingCloudRecordReader, MarketingCloudRecordReader.class.getDeclaredField("tableFields"),
                         list);
    FieldSetter.setField(marketingCloudRecordReader, MarketingCloudRecordReader.class.getDeclaredField("row"),
                         row);
    FieldSetter.setField(marketingCloudRecordReader, MarketingCloudRecordReader.class.getDeclaredField
      ("sfObjectMetaData"), sfObjectMetaData);
    Assert.assertNotNull(marketingCloudRecordReader.getCurrentValue());
  }

  @Test
  public void testConvertToValueWRecord() {
    Schema fieldSchema = Schema.recordOf("record",
                                         Schema.Field.of("store_id", Schema.of(Schema.Type.INT)),
                                         Schema.Field.of("markedPrice", Schema.nullableOf(Schema.decimalOf
                                           (4, 2))),
                                         Schema.Field.of("timestamp",
                                                         Schema.nullableOf(Schema.of
                                                           (Schema.LogicalType.TIMESTAMP_MICROS))),
                                         Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                         Schema.Field.of("bytes", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));
    thrown.expect(IllegalStateException.class);
    Assert.assertNotNull(marketingCloudRecordReader.convertToValue("store_id", fieldSchema,
                                                                   new HashMap<>(1)));
  }

  @Test
  public void testConvertToValueWInvalidFieldName() {
    Schema fieldSchema = Schema.recordOf("record",
                                         Schema.Field.of("store_id", Schema.of(Schema.Type.NULL)),
                                         Schema.Field.of("emailid", Schema.of(Schema.Type.NULL)));
    thrown.expect(IllegalStateException.class);
    Assert.assertNotNull(marketingCloudRecordReader.convertToValue("Field Name", fieldSchema, new HashMap<>(1)));
  }

  @Test
  public void testConvertToStringValue() {
    Assert.assertEquals("Field Value", marketingCloudRecordReader.convertToStringValue("Field Value"));
  }

  @Test
  public void testConvertToDoubleValue() {
    Assert.assertEquals(42.0, marketingCloudRecordReader.convertToDoubleValue("42").doubleValue(), 0.0);
    Assert.assertEquals(42.0, marketingCloudRecordReader.convertToDoubleValue(42).doubleValue(), 0.0);
    Assert.assertNull(marketingCloudRecordReader.convertToDoubleValue(""));
  }

  @Test
  public void testConvertToIntegerValue() {
    Assert.assertEquals(42, marketingCloudRecordReader.convertToIntegerValue("42").intValue());
    Assert.assertEquals(42, marketingCloudRecordReader.convertToIntegerValue(42).intValue());
    Assert.assertNull(marketingCloudRecordReader.convertToIntegerValue(""));
  }

  @Test
  public void testConvertToBooleanValue() {
    Assert.assertFalse(marketingCloudRecordReader.convertToBooleanValue("Field Value"));
    Assert.assertFalse(marketingCloudRecordReader.convertToBooleanValue(42));
    Assert.assertNull(marketingCloudRecordReader.convertToBooleanValue(""));
  }
}
