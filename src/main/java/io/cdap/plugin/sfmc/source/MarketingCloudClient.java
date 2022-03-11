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
import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.Util;
import jline.internal.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
    conf.set("cxfConnectTimeout", "300000");
    conf.set("cxfReceiveTimeout", "300000");
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
  public ETResponse<? extends ETSoapObject> fetchObjectRecords(SourceObject object,
                                                               @Nullable String requestId) throws ETSdkException {
    ETFilter filter = new ETFilter();
    filter.setExpression(getExpressionfromString(object.getFilter()));
    return fetchObjectData(client, object.getClassRef(), filter, requestId);
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
  public ETResponse<ETDataExtensionRow> fetchDataExtensionRecords(String dataExtensionKey, String filterStr,
                                                                  @Nullable String requestId) throws ETSdkException {

    ETFilter filter = new ETFilter();
    filter.setExpression(getExpressionfromString(filterStr));
    ETResponse<ETDataExtensionRow> response = null;

    if (requestId == null) {
      response = PaginationETSoapObject.select(client, "key=" + dataExtensionKey,
                                               filter);
    } else {
      response = PaginationETSoapObject.continueRequest(client, null, requestId,
                                                        filter);
    }
    return response;
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

  private <T extends ETSoapObject> ETResponse<T> fetchObjectData(ETClient client, Class<T> clazz, ETFilter filter,
                                                                 @Nullable String requestId) throws ETSdkException {

    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(MarketingCloudClient.class.getClassLoader());
      ETResponse<T> response = null;

      if (requestId == null) {
        response = PaginationETSoapObject.customRetrieve(client, null,
                                                         filter, null,
                                                         clazz);
      } else {
        response = PaginationETSoapObject.customRetrieve(client, null,
                                                         filter, requestId, clazz);
      }
      return response;
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }


  private List<MarketingCloudColumn> fetchObjectFields(Class<?> clazz) {
    return Arrays.stream(clazz.getDeclaredFields())
      .map(o -> new MarketingCloudColumn(o.getName(), o.getType().getSimpleName().toUpperCase()))
      .collect(Collectors.toList());
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
