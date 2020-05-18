/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETConfiguration;
import com.exacttarget.fuelsdk.ETDataExtension;
import com.exacttarget.fuelsdk.ETDataExtensionColumn;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETExpression;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.plugin.sfmc.source.util.SalesforceColumn;
import io.cdap.plugin.sfmc.source.util.SalesforceConstants;
import io.cdap.plugin.sfmc.source.util.SalesforceObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class that provides methods to connect to Salesforce instance.
 */
public class SalesforceClient {
  private final ETClient client;

  SalesforceClient(ETClient client) {
    this.client = client;
  }

  /**
   * Initialize the connection with Salesforce Marketing Cloud using FuelSDK.
   *
   * @param clientId The Salesforce Marketing Cloud Client Id
   * @param clientSecret The Salesforce Marketing Cloud Client Secret
   * @param authEndpoint Auth Endpoint url for Salesforce Marketing Cloud
   * @param soapEndpoint SOAP Endpoint url for Salesforce Marketing Cloud
   * @return The instance of SalesforceClient object
   * @throws ETSdkException The FuelSDKException
   */
  public static SalesforceClient create(String clientId, String clientSecret, String authEndpoint,
                                        String soapEndpoint) throws ETSdkException {
    ETConfiguration conf = new ETConfiguration();
    conf.set("clientId", clientId);
    conf.set("clientSecret", clientSecret);
    conf.set("authEndpoint", authEndpoint);
    conf.set("soapEndpoint", soapEndpoint);
    conf.set("useOAuth2Authentication", "true");
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(SalesforceClient.class.getClassLoader());
      return new SalesforceClient(new ETClient(conf));
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  /**
   * Fetch records for passed object from Salesforce Marketing Cloud.
   *
   * @param object The SourceObject which tells what data to be fetched from Salesforce Marketing Cloud
   * @param page The page index
   * @param pageSize The size of the page
   * @return The list of ETApiObject representing the records from requested object
   */
  public List<? extends ETApiObject> fetchObjectRecords(SourceObject object, int page, int pageSize) {
    try {
      Integer pageIndex = null;
      Integer maxPageSize = null;

      if (object.isPagingSupported()) {
        pageIndex = page;
        maxPageSize = pageSize;
      }

      return fetchObjectData(client, object.getClassRef(), pageIndex, maxPageSize);
    } catch (ETSdkException e) {
      return Collections.emptyList();
    }
  }

  /**
   * Fetch the schema information for passed object from Salesforce Marketing Cloud.
   *
   * @param object The SourceObject which tells for which schema to be fetched from Salesforce Marketing Cloud
   * @param fetchRecordCount the flag to decide whether to fetch record count or not
   * @return The instance of SalesforceObjectInfo object
   * @throws ETSdkException The FuelSDKException
   */
  public SalesforceObjectInfo fetchObjectSchema(SourceObject object, boolean fetchRecordCount) throws ETSdkException {
    Class<? extends ETApiObject> clazz = object.getClassRef();
    Integer recordCount = SalesforceConstants.MAX_PAGE_SIZE;

    if (object.isPagingSupported() && fetchRecordCount) {
      ETResponse<? extends ETApiObject> etResponse = client.retrieve(clazz, 1, 1, new ETFilter());
      recordCount = etResponse.getTotalCount();
    }

    return new SalesforceObjectInfo(object, fetchObjectFields(clazz), recordCount);
  }

  /**
   * Fetch records for passed object from Salesforce Marketing Cloud.
   *
   * @param dataExtensionKey The data extension key for which data to be fetched from Salesforce Marketing Cloud
   * @param page The page index
   * @param pageSize The size of the page
   * @return The list of ETDataExtensionRow representing the records from requested data extension
   * @throws ETSdkException The FuelSDKException
   */
  public List<ETDataExtensionRow> fetchDataExtensionRecords(String dataExtensionKey, int page, int pageSize)
    throws ETSdkException {
    return call(() -> ETDataExtension.select(client, "key=" + dataExtensionKey, page, pageSize)
      .getObjects());
  }

  /**
   * Fetch the schema information for passed object from Salesforce Marketing Cloud.
   *
   * @param dataExtensionKey The data extension key for which schema to be fetched from Salesforce Marketing Cloud
   * @param fetchRecordCount the flag to decide whether to fetch record count or not
   * @return The instance of SalesforceObjectInfo object
   * @throws ETSdkException The FuelSDKException
   */
  public SalesforceObjectInfo fetchDataExtensionSchema(String dataExtensionKey, boolean fetchRecordCount)
    throws ETSdkException {
    return call(() -> {
      ETExpression expression = buildDataExtensionExpression(dataExtensionKey);

      ETFilter filter = new ETFilter();
      filter.setExpression(expression);
      filter.addProperty("name");
      filter.addProperty("type");

      ETResponse<ETDataExtensionColumn> response = ETDataExtensionColumn.retrieve(client,
        ETDataExtensionColumn.class, (Integer) null, (Integer) null, filter);
      List<SalesforceColumn> columns = response.getObjects().stream()
        .map(o -> new SalesforceColumn(o.getName(), o.getType().name()))
        .collect(Collectors.toList());

      Integer recordCount = fetchDataExtensionRecordCount(dataExtensionKey);

      return new SalesforceObjectInfo(SourceObject.DATA_EXTENSION, dataExtensionKey, columns, recordCount);
    });
  }

  private Integer fetchDataExtensionRecordCount(String dataExtensionKey) throws ETSdkException {
    Integer totalCount = 0;
    Integer pageRecordCount;
    int page = 1;

    do {
      pageRecordCount = ETDataExtension.select(client, "key=" + dataExtensionKey, page,
                                               SalesforceConstants.MAX_PAGE_SIZE).getResults().size();
      totalCount += pageRecordCount;
      page++;
    } while (pageRecordCount >= SalesforceConstants.MAX_PAGE_SIZE);

    return totalCount;
  }

  private ETExpression buildDataExtensionExpression(String dataExtensionKey) {
    ETExpression expression = new ETExpression();

    expression.setProperty("DataExtension.CustomerKey");
    expression.setOperator(ETExpression.Operator.EQUALS);
    expression.addValue(dataExtensionKey);

    return expression;
  }

  private <T extends ETApiObject> List<T> fetchObjectData(ETClient client, Class<T> clazz, Integer page,
                                                          Integer pageSize)
    throws ETSdkException {
    ETResponse<T> etResponse = client.retrieve(clazz, page, pageSize, new ETFilter());
    return etResponse.getObjects();
  }

  private List<SalesforceColumn> fetchObjectFields(Class<?> clazz) {
    return Arrays.stream(clazz.getDeclaredFields())
      .map(o -> new SalesforceColumn(o.getName(), o.getType().getSimpleName().toUpperCase()))
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
