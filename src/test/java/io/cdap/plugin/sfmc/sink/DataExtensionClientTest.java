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
import com.exacttarget.fuelsdk.ETConfiguration;
import com.exacttarget.fuelsdk.ETDataExtension;
import com.exacttarget.fuelsdk.ETDataExtensionColumn;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETResult;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.sfmc.source.MarketingCloudSourceConfig;

import org.apache.cxf.frontend.ClientProxy;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.xml.soap.SOAPFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ETDataExtension.class, ClientProxy.class, SOAPFactory.class, DataExtensionClient.class,
  ClassLoader.class})
public class DataExtensionClientTest {

  protected static final String CLIENT_ID = "clientId";
  protected static final String CLIENT_SECRET = "clientSecret";
  protected static final String AUTH_ENDPOINT = "authEndPoint";
  protected static final String SOAP_ENDPOINT = "soapEndPoint";
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  MarketingCloudSourceConfig config = new MarketingCloudSourceConfig("referenceName", "queryMode", "open Event",
    "dataExtensionKey", "objectList",
    "dataExtensionKeys", "tableNameField", "filter", CLIENT_ID, CLIENT_SECRET,
    "restEndpoint", AUTH_ENDPOINT, SOAP_ENDPOINT);


  public ETClient getClient() throws ETSdkException {
    ETConfiguration conf = new ETConfiguration();
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(DataExtensionClient.class.getClassLoader());
    ETClient client = Mockito.mock(ETClient.class);
    return client;

  }

  @Test
  public void testDataExtensionKey() throws ETSdkException {


    DataExtensionClient dataExtensionClient = Mockito.mock(DataExtensionClient.class);
    Mockito.when(dataExtensionClient.getDataExtensionKey()).thenReturn("DE");
    Assert.assertFalse(dataExtensionClient.getDataExtensionKey().isEmpty());

  }


  @Test
  public void testValidateSchemaCompatibility() throws ETSdkException {
    MockFailureCollector collector = new MockFailureCollector();
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("markedPrice", Schema.nullableOf(Schema.decimalOf(4, 2))),
      Schema.Field.of("updated", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("totalProducts", Schema.of(Schema.Type.INT)),
      Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("null", Schema.of(Schema.Type.NULL)),
      Schema.Field.of("timestamp",
        Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    Collection<ETDataExtensionColumn> columns = new ArrayList<>();
    ETDataExtensionColumn column1 = new ETDataExtensionColumn();
    column1.setId("121");
    column1.setDescription("newCustomer");
    column1.setLength(22);
    column1.setName("name");
    ETDataExtensionColumn column2 = new ETDataExtensionColumn();
    Date d = new Date(2022, 02, 9);
    column2.setCreatedDate(d);
    column2.setDefaultValue("default");
    column2.setName("dt");
    column2.setType(ETDataExtensionColumn.Type.DECIMAL);
    ETDataExtensionColumn column3 = new ETDataExtensionColumn();
    column3.setIsPrimaryKey(true);
    column3.setIsRequired(true);
    column3.setKey("key");
    column3.setName("updated");
    column3.setType(ETDataExtensionColumn.Type.NUMBER);
    ETDataExtensionColumn column4 = new ETDataExtensionColumn();
    column4.setModifiedDate(d);
    column4.setName("id");
    column4.setType(ETDataExtensionColumn.Type.LOCALE);
    ETDataExtensionColumn column6 = new ETDataExtensionColumn();
    column6.setModifiedDate(d);
    column6.setName("null");
    column6.setType(ETDataExtensionColumn.Type.BOOLEAN);
    ETDataExtensionColumn column7 = new ETDataExtensionColumn();
    column7.setModifiedDate(d);
    column7.setName("markedPrice");
    column7.setType(ETDataExtensionColumn.Type.DATE);
    columnList.add(column1);
    columnList.add(column2);
    columnList.add(column3);
    columnList.add(column4);
    columnList.add(column6);
    columnList.add(column7);
    columns.add(column7);
    columns.add(column1);
    columns.add(column2);
    columns.add(column3);
    columns.add(column4);
    columns.add(column6);
    String dataExtensionKey = "DE";
    PowerMockito.mockStatic(ETDataExtension.class);
    DataExtensionClient dataExtensionClient = Mockito.spy(new DataExtensionClient(getClient(), dataExtensionKey));
    PowerMockito.when(ETDataExtension.retrieveColumns(Mockito.any(), Mockito.anyString())).thenReturn(columnList);
    try {
      dataExtensionClient.validateSchemaCompatibility(schema, collector);
      collector.getOrThrowException();

    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation.", e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaCompatibilityWNullColumns() throws ETSdkException {
    MockFailureCollector collector = new MockFailureCollector();
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("timestamp",
        Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    String dataExtensionKey = "DE";
    PowerMockito.mockStatic(ETDataExtension.class);
    DataExtensionClient dataExtensionClient = Mockito.spy(new DataExtensionClient(getClient(), dataExtensionKey));
    Mockito.when(dataExtensionClient.getDataExtensionInfo()).thenReturn(null);
    PowerMockito.when(ETDataExtension.retrieveColumns(Mockito.any(), Mockito.anyString())).thenReturn(columnList);
    try {
      dataExtensionClient.validateSchemaCompatibility(schema, collector);
      collector.getOrThrowException();

    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation.", e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaCompatibilityWNullSchemaField() throws ETSdkException {
    MockFailureCollector collector = new MockFailureCollector();
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("timestamp",
        Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));
    List<ETDataExtensionColumn> columnList = new ArrayList<>();
    Collection<ETDataExtensionColumn> columns = new ArrayList<>();
    ETDataExtensionColumn column = new ETDataExtensionColumn();
    Date d = new Date(2022, 02, 9);
    column.setCreatedDate(d);
    column.setDefaultValue("default");
    column.setName("dt");
    columns.add(column);
    String dataExtensionKey = "DE";
    columnList.add(column);
    column.setIsRequired(true);
    PowerMockito.mockStatic(ETDataExtension.class);
    DataExtensionClient dataExtensionClient = Mockito.spy(new DataExtensionClient(getClient(), dataExtensionKey));
    PowerMockito.when(ETDataExtension.retrieveColumns(Mockito.any(), Mockito.anyString())).thenReturn(columnList);
    try {
      dataExtensionClient.validateSchemaCompatibility(schema, collector);
      collector.getOrThrowException();

    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation.", e.getMessage());
    }
  }

  @Test
  public void testScan() throws ETSdkException {
    String dataExtensionKey = "DE";
    List<ETDataExtensionRow> row = new ArrayList<>();
    ETDataExtensionRow row1 = new ETDataExtensionRow();
    row1.setId("id");
    row.add(row1);
    ETResponse<ETDataExtensionRow> response = new ETResponse<>();
    response.setStatus(ETResult.Status.ERROR);
    PowerMockito.mockStatic(ETDataExtension.class);
    PowerMockito.when(ETDataExtension.select((ETClient) Mockito.any(), Mockito.anyString())).thenReturn(response);
    DataExtensionClient dataExtensionClient = Mockito.spy(new DataExtensionClient(getClient(), dataExtensionKey));
    dataExtensionClient.scan();
  }


}













