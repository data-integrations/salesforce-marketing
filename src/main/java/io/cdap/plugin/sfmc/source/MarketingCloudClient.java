/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.custom.fuelsdk.PaginationETSoapObject;
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETConfiguration;
import com.exacttarget.fuelsdk.ETDataExtensionColumn;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETExpression;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.annotations.SoapObject;
import com.exacttarget.fuelsdk.internal.APIObject;
import com.exacttarget.fuelsdk.internal.ArrayOfObjectDefinitionRequest;
import com.exacttarget.fuelsdk.internal.DefinitionRequestMsg;
import com.exacttarget.fuelsdk.internal.DefinitionResponseMsg;
import com.exacttarget.fuelsdk.internal.ObjectDefinition;
import com.exacttarget.fuelsdk.internal.ObjectDefinitionRequest;
import com.exacttarget.fuelsdk.internal.PropertyDefinition;
import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class that provides methods to connect to Salesforce instance.
 */
public class MarketingCloudClient {
  private final ETClient client;
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudClient.class);

  MarketingCloudClient(ETClient client) {
    this.client = client;
  }

  /**
   * Initialize the connection with Salesforce Marketing Cloud using FuelSDK.
   *
   * @param clientId     The Salesforce Marketing Cloud Client Id
   * @param clientSecret The Salesforce Marketing Cloud Client Secret
   * @param authEndpoint Auth Endpoint url for Salesforce Marketing Cloud
   * @param soapEndpoint SOAP Endpoint url for Salesforce Marketing Cloud
   * @return The instance of MarketingCloudClient object
   * @throws ETSdkException The FuelSDKException
   */
  public static MarketingCloudClient create(String clientId, String clientSecret, String authEndpoint,
                                            String soapEndpoint) throws ETSdkException {
    ETConfiguration conf = new ETConfiguration();
    conf.set("clientId", clientId);
    conf.set("clientSecret", clientSecret);
    conf.set("authEndpoint", authEndpoint);
    conf.set("soapEndpoint", soapEndpoint);
    conf.set("useOAuth2Authentication", "true");
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(MarketingCloudClient.class.getClassLoader());
      return new MarketingCloudClient(new ETClient(conf));
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  /**
   * Check if filter string is a valid SFMC Expression.
   *
   * @param filter
   * @return
   */
  public static void validateFilter(String filter) throws ETSdkException {
    if (!Util.isNullOrEmpty(filter)) {
      ETExpression expression = ETExpression.parse(filter);
    }
  }

  private ETExpression getExpressionfromString(String filter) throws ETSdkException {
    if (!Util.isNullOrEmpty(filter)) {
      return ETExpression.parse(filter);
    } else {
      return new ETExpression();
    }
  }

  /**
   * Fetch records for passed object from Salesforce Marketing Cloud.
   *
   * @param object The SourceObject which tells what data to be fetched from Salesforce Marketing Cloud
   * @return The list of ETApiObject representing the records from requested object
   */
  public List<? extends ETSoapObject> fetchObjectRecords(SourceObject object) throws ETSdkException {
    ETFilter filter = new ETFilter();
    filter.setExpression(getExpressionfromString(object.getFilter()));
    return fetchObjectData(client, object.getClassRef(), filter);
  }

  /**
   * Fetch the schema information for passed object from Salesforce Marketing Cloud.
   *
   * @param object The SourceObject which tells for which schema to be fetched from Salesforce Marketing Cloud
   * @return The instance of MarketingCloudObjectInfo object
   * @throws ETSdkException The FuelSDKException
   */
  public MarketingCloudObjectInfo fetchObjectSchema(SourceObject object)
    throws ETSdkException {
    Class<? extends ETSoapObject> clazz = object.getClassRef();
    return new MarketingCloudObjectInfo(object, fetchObjectFields(clazz));
  }

  /**
   * Fetch records for passed object from Salesforce Marketing Cloud.
   *
   * @param dataExtensionKey The data extension key for which data to be fetched from Salesforce Marketing Cloud
   * @return The list of ETDataExtensionRow representing the records from requested data extension
   * @throws ETSdkException The FuelSDKException
   */
  public List<ETDataExtensionRow> fetchDataExtensionRecords(String dataExtensionKey, String filterStr)
    throws ETSdkException {

    ETFilter filter = new ETFilter();
    filter.setExpression(getExpressionfromString(filterStr));

    ETResponse<ETDataExtensionRow> response = PaginationETSoapObject.select(client, "key=" + dataExtensionKey,
                                                                            filter);
    List<ETDataExtensionRow> rows = response.getObjects();
    while (response.getResponseMessage().equals("MoreDataAvailable")) {
      response = PaginationETSoapObject.continueRequest(client, null, response.getRequestId(),
                                                        filter);
      rows.addAll(response.getObjects());
    }
    return rows;
  }

  /**
   * Fetch the schema information for passed object from Salesforce Marketing Cloud.
   *
   * @param dataExtensionKey The data extension key for which schema to be fetched from Salesforce Marketing Cloud
   * @return The instance of MarketingCloudObjectInfo object
   * @throws ETSdkException The FuelSDKException
   */
  public MarketingCloudObjectInfo fetchDataExtensionSchema(String dataExtensionKey)
    throws ETSdkException {
    return call(() -> {
      ETExpression expression = buildDataExtensionExpression(dataExtensionKey);

      ETFilter filter = new ETFilter();
      filter.setExpression(expression);
      filter.addProperty("name");
      filter.addProperty("type");

      ETResponse<ETDataExtensionColumn> response = ETDataExtensionColumn.retrieve(client, ETDataExtensionColumn.class,
                                                                                  (Integer) null, (Integer) null,
                                                                                  filter);
      List<MarketingCloudColumn> columns = response.getObjects().stream()
        .map(o -> new MarketingCloudColumn(o.getName(), o.getType().name()))
        .collect(Collectors.toList());

      return new MarketingCloudObjectInfo(SourceObject.DATA_EXTENSION, dataExtensionKey, columns);
    });
  }

  private ETExpression buildDataExtensionExpression(String dataExtensionKey) {
    ETExpression expression = new ETExpression();
    expression.setProperty("DataExtension.CustomerKey");
    expression.setOperator(ETExpression.Operator.EQUALS);
    expression.addValue(dataExtensionKey);
    return expression;
  }

  private <T extends ETSoapObject> List<T> fetchObjectData(ETClient client, Class<T> clazz, ETFilter filter)
    throws ETSdkException {

    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(MarketingCloudClient.class.getClassLoader());
      ETResponse<T> response = (ETResponse<T>) PaginationETSoapObject.customRetrieve(client, null,
                                                                                     filter, null,
                                                                                     clazz);
      List<T> rows = response.getObjects();
      while (response.getResponseMessage().equals("MoreDataAvailable")) {
        response = (ETResponse<T>) PaginationETSoapObject.customRetrieve(client, null,
                                                                         filter, response.getRequestId(),
                                                                         clazz);
        rows.addAll(response.getObjects());
      }
      return rows;
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }


  private List<MarketingCloudColumn> fetchObjectFields(Class<?> clazz) {
    SoapObject internalTypeAnnotation
            = clazz.getAnnotation(SoapObject.class);
    assert internalTypeAnnotation != null;
    Class<? extends APIObject> internalType = internalTypeAnnotation.internalType();
    assert internalType != null;
    List<MarketingCloudColumn> marketingCloudColumns = new ArrayList<>();
    MarketingCloudColumn marketingCloudColumn = new MarketingCloudColumn();
    DefinitionRequestMsg definitionRequestMsg = new DefinitionRequestMsg();
    List<ObjectDefinitionRequest> objectDefinitionRequests = new ArrayList<>();
    ObjectDefinitionRequest objectDefinitionRequest = new ObjectDefinitionRequest();
    objectDefinitionRequest.setObjectType(internalType.getSimpleName());
    objectDefinitionRequests.add(objectDefinitionRequest);
    ArrayOfObjectDefinitionRequest arrayOfObjectDefinitionRequest = new ArrayOfObjectDefinitionRequest();
    arrayOfObjectDefinitionRequest.getObjectDefinitionRequest().add(objectDefinitionRequest);
    definitionRequestMsg.setDescribeRequests(arrayOfObjectDefinitionRequest);

    DefinitionResponseMsg definitionResponseMsg = client.getSoapConnection().getSoap().describe(definitionRequestMsg);

    for (ObjectDefinition result : definitionResponseMsg.getObjectDefinition()) {
      for (PropertyDefinition propertyDefinition : result.getProperties()) {
        if (propertyDefinition.getIsRetrievable() && !propertyDefinition.getName().contains(".")) {
          marketingCloudColumn = new MarketingCloudColumn(propertyDefinition.getName(),
                  propertyDefinition.getDataType());
          marketingCloudColumns.add(marketingCloudColumn);
        }
      }
    }
    return marketingCloudColumns;
  }

  private <T> T call(SFMCCall<T> callable) throws ETSdkException {
    ClassLoader oldClassloader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return callable.call();
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassloader);
    }
  }

  /**
   * A SFMC call.
   *
   * @param <T> type of return object
   */
  private interface SFMCCall<T> {

    /**
     * Perform a call.
     */
    T call() throws ETSdkException;
  }
}
