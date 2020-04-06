/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import io.cdap.plugin.sfmc.source.util.SalesforceObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class that provides methods to connect to Salesforce instance.
 */
public class SalesforceClient {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceClient.class);
  private final ETClient client;

  SalesforceClient(ETClient client) {
    this.client = client;
  }

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

  public List<? extends ETApiObject> fetchObjectRecords(SourceObject object) {
    try {
      return fetchObjectData(client, object.getClassRef());
    } catch (ETSdkException e) {
      return Collections.emptyList();
    }
  }

  public SalesforceObjectInfo fetchObjectSchema(SourceObject object) {
    return new SalesforceObjectInfo(object, fetchObjectFields(object.getClassRef()), 0);
  }

  public List<ETDataExtensionRow> fetchDataExtensionRecords(String dataExtensionKey) throws ETSdkException {
    return call(() -> {
      ETExpression expression = buildDataExtensionExpression(dataExtensionKey);

      ETFilter filter = new ETFilter();
      filter.addProperty("key");
      filter.addProperty("name");
      filter.setExpression(expression);

      ETResponse<ETDataExtension> response = client.retrieve(ETDataExtension.class, filter);
      List<ETDataExtension> extensionRows = response.getObjects();

      if (extensionRows.isEmpty()) {
        return Collections.emptyList();
      }

      ETDataExtension de = extensionRows.get(0);
      return de.select().getObjects();
    });
  }

  public SalesforceObjectInfo fetchDataExtensionSchema(String dataExtensionKey) throws ETSdkException {
    return call(() -> {
      ETExpression expression = buildDataExtensionExpression(dataExtensionKey);

      ETFilter filter = new ETFilter();
      filter.setExpression(expression);
      filter.addProperty("name");
      filter.addProperty("type");

      ETResponse<ETDataExtensionColumn> response = ETDataExtensionColumn.retrieve(client, ETDataExtensionColumn.class,
        (Integer) null, (Integer) null, filter);
      List<SalesforceColumn> columns = response.getObjects().stream()
        .map(o -> new SalesforceColumn(o.getName(), o.getType().name()))
        .collect(Collectors.toList());

      return new SalesforceObjectInfo(SourceObject.DATA_EXTENSION, dataExtensionKey, columns, 0);
    });
  }

  private ETExpression buildDataExtensionExpression(String dataExtensionKey) {
    ETExpression expression = new ETExpression();

    expression.setProperty("DataExtension.CustomerKey");
    expression.setOperator(ETExpression.Operator.EQUALS);
    expression.addValue(dataExtensionKey);

    return expression;
  }

  private <T extends ETApiObject> List<T> fetchObjectData(ETClient client, Class<T> clazz) throws ETSdkException {
    ETResponse<T> etResponse = client.retrieve(clazz, new ETFilter());
    List<T> rows = etResponse.getObjects();

    return rows;
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
   * A SFMC call
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
