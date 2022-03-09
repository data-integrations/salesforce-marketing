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
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
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
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class MarketingCloudRecordReaderTest {

  protected static final String CLIENT_ID = System.getProperty("sfmc.test.clientId");
  protected static final String CLIENT_SECRET = System.getProperty("sfmc.test.clientSecret");
  protected static final String AUTH_ENDPOINT = System.getProperty("sfmc.test.authEndpoint");
  protected static final String SOAP_ENDPOINT = System.getProperty("sfmc.test.soapEndpoint");
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
  public void testFetchDataWithoutDataExtension() throws ETSdkException, IOException {
    List<Schema.Field> tableFields = new ArrayList<>();
    List<Schema.Field> schemaFields = new ArrayList<>();
    List<? extends ETApiObject> results = new ArrayList<>();
    MarketingCloudObjectInfo sfObjectMetaData;
    MarketingCloudInputSplit split = new MarketingCloudInputSplit("TRACKING_UNSUB_EVENT", "unsub");
    MarketingCloudRecordReader marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
    SourceObject object = SourceObject.valueOf(split.getObjectName());
    String tableName = split.getTableName();
    MarketingCloudClient client = MarketingCloudClient.create(marketingCloudSourceConfig.getClientId(),
      marketingCloudSourceConfig.getClientSecret(),
      marketingCloudSourceConfig.getAuthEndpoint(), marketingCloudSourceConfig.getSoapEndpoint());
    results = client.fetchObjectRecords(object);
    sfObjectMetaData = client.fetchObjectSchema(object);
    tableFields = sfObjectMetaData.getSchema().getFields();
    schemaFields = new ArrayList<>(tableFields);
    Schema schema = Schema.recordOf(tableName.replaceAll("-", "_"), schemaFields);
    marketingCloudRecordReader.initialize(split, null);
    Assert.assertTrue(marketingCloudRecordReader.nextKeyValue());
    Assert.assertFalse(schema.isNullable());
    Assert.assertEquals(schema.getFields().size(), 10);


  }

  /*@Test
  public void testFetchDataWithoutClient() throws ETSdkException, IOException {
    List<Schema.Field> tableFields = new ArrayList<>();
    List<Schema.Field> schemaFields = new ArrayList<>();
    List<? extends ETApiObject> results = new ArrayList<>();
    MarketingCloudInputSplit split = new MarketingCloudInputSplit("TRACKING_UNSUB_EVENT", "unsub");
    MarketingCloudRecordReader marketingCloudRecordReader = new MarketingCloudRecordReader(marketingCloudSourceConfig);
    SourceObject object = SourceObject.valueOf(split.getObjectName());
    String tableName = split.getTableName();
    MarketingCloudClient client = Mockito.mock(MarketingCloudClient.class);
    ETClickEvent clickEvent = new ETClickEvent();
    results.add(clickEvent);


  }*/


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

