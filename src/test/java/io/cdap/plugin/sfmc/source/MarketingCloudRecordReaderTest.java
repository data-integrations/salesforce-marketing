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
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETConfiguration;
import com.exacttarget.fuelsdk.ETSoapConnection;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.internal.EventType;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
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
@PrepareForTest({ETClient.class, ClassLoader.class, MarketingCloudClient.class, ETSoapConnection.class})
public class MarketingCloudRecordReaderTest {

  protected static final String CLIENT_ID = "clientId";
  protected static final String CLIENT_SECRET = "clientSecret";
  protected static final String AUTH_ENDPOINT = "authEndPoint";
  protected static final String SOAP_ENDPOINT = "soapEndPoint";
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudInputFormat.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private MarketingCloudSourceConfig marketingCloudSourceConfig;
  private MarketingCloudRecordReader marketingCloudRecordReader;


  @Before
  public void initialize() {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, AUTH_ENDPOINT, SOAP_ENDPOINT);
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
      marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
    } catch (AssumptionViolatedException e) {
      LOG.warn("MarketingCloud batch Source tests are skipped. ");
      throw e;
    }
  }

  @Test
  public void testConstructor() throws IOException {
    marketingCloudRecordReader.close();
    Assert.assertEquals(0, marketingCloudRecordReader.pos);
  }


  @Test
  public void testConstructor2() throws IOException {

    MarketingCloudSourceConfig marketingCloudSourceConfig = new MarketingCloudSourceConfig("referenceName",
      "Single Object",
      "Data Extension",
      "DE", "objectList",
      null, "tableNameField",
      "filter", CLIENT_ID, CLIENT_SECRET, "restEndPoint", AUTH_ENDPOINT, SOAP_ENDPOINT);
    Assert.assertEquals("restEndPoint", marketingCloudSourceConfig.getRestEndpoint());
    Assert.assertEquals("DE", marketingCloudSourceConfig.getDataExtensionKey());
    Assert.assertEquals(SourceQueryMode.SINGLE_OBJECT, marketingCloudSourceConfig.getQueryMode());
    Assert.assertEquals(null, marketingCloudSourceConfig.getDataExtensionKeys());
    Assert.assertEquals("tableNameField", marketingCloudSourceConfig.getTableNameField());
    Assert.assertEquals(SourceObject.DATA_EXTENSION, marketingCloudSourceConfig.getObject());
    Assert.assertEquals("referenceName", marketingCloudSourceConfig.getReferenceName());
    PluginProperties properties = marketingCloudSourceConfig.getProperties();
    Assert.assertTrue(properties.getProperties().isEmpty());
    marketingCloudRecordReader.close();
    Assert.assertEquals(0, marketingCloudRecordReader.pos);
  }

  @Test
  public void testFetchData() throws Exception {
    MarketingCloudInputSplit split = new MarketingCloudInputSplit("TRACKING_UNSUB_EVENT", "unsub");
    SourceObject object = SourceObject.valueOf(split.getObjectName());
    MarketingCloudRecordReader marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
    List<? extends ETSoapObject> results = new ArrayList<ETSoapObject>();
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
    Mockito.doReturn(results).when(client).fetchObjectRecords(object);
    marketingCloudRecordReader.initialize(split, null);
    Assert.assertTrue(marketingCloudRecordReader.nextKeyValue());


  }


  @Test
  public void testConvertToValueBooleanType() {

    Schema fieldSchema = Schema.of(Schema.Type.BOOLEAN);
    marketingCloudRecordReader.convertToValue("Field Name", fieldSchema, new HashMap<>(1));
  }

  @Test
  public void testConvertToValueStringFieldType() {
    Schema fieldSchema = Schema.of(Schema.Type.STRING);
    marketingCloudRecordReader.convertToValue("Field Name", fieldSchema, new HashMap<>(1));
  }


  @Test
  public void testConvertToValueWRecord() {
    Schema fieldSchema = Schema.recordOf("record",
      Schema.Field.of("store_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("emailid", Schema.of(Schema.Type.STRING)));

    thrown.expect(IllegalStateException.class);
    marketingCloudRecordReader.convertToValue("store_id", fieldSchema, new HashMap<>(1));
  }

  @Test
  public void testConvertToValueWInvalidFieldName() {
    Schema fieldSchema = Schema.recordOf("record",
      Schema.Field.of("store_id", Schema.of(Schema.Type.NULL)),
      Schema.Field.of("emailid", Schema.of(Schema.Type.NULL)));
    thrown.expect(IllegalStateException.class);
    marketingCloudRecordReader.convertToValue("Field Name", fieldSchema, new HashMap<>(1));

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

