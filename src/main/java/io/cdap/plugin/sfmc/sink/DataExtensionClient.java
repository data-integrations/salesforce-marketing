/*
 * Copyright © 2019 Cask Data, Inc.
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
import com.exacttarget.fuelsdk.ETSoapConnection;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.internal.APIObject;
import com.exacttarget.fuelsdk.internal.CreateOptions;
import com.exacttarget.fuelsdk.internal.CreateRequest;
import com.exacttarget.fuelsdk.internal.CreateResponse;
import com.exacttarget.fuelsdk.internal.CreateResult;
import com.exacttarget.fuelsdk.internal.DataExtensionCreateResult;
import com.exacttarget.fuelsdk.internal.DataExtensionUpdateResult;
import com.exacttarget.fuelsdk.internal.Soap;
import com.exacttarget.fuelsdk.internal.UpdateOptions;
import com.exacttarget.fuelsdk.internal.UpdateRequest;
import com.exacttarget.fuelsdk.internal.UpdateResponse;
import com.exacttarget.fuelsdk.internal.UpdateResult;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Wrapper around an ETClient that understands objects at the level that the plugin cares about.
 */
public class DataExtensionClient {
  private final ETClient client;
  private final String dataExtensionKey;

  DataExtensionClient(ETClient client, String dataExtensionKey) {
    this.client = client;
    this.dataExtensionKey = dataExtensionKey;
  }

  public static DataExtensionClient create(String dataExtensionKey, String clientId, String clientSecret,
                                           String authEndpoint, String soapEndpoint) throws ETSdkException {
    ETConfiguration conf = new ETConfiguration();
    conf.set("clientId", clientId);
    conf.set("clientSecret", clientSecret);
    conf.set("authEndpoint", authEndpoint);
    conf.set("soapEndpoint", soapEndpoint);
    conf.set("useOAuth2Authentication", "true");
    ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(DataExtensionClient.class.getClassLoader());
      return new DataExtensionClient(new ETClient(conf), dataExtensionKey);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCL);
    }
  }

  public String getDataExtensionKey() {
    return dataExtensionKey;
  }

  /**
   * Validate that the given schema is compatible with the given extension.
   *
   * @param schema the schema to check compatibility with
   * @param collector failure collector
   * @throws ETSdkException if there was an error getting the column information for the data extension
   */
  public void validateSchemaCompatibility(Schema schema, FailureCollector collector) throws ETSdkException {
    call(() -> {
      Collection<ETDataExtensionColumn> columns = getDataExtensionInfo().getColumnList();
      if (columns == null || columns.isEmpty()) {
        collector.addFailure(String.format("Data extension '%s' must exist.", dataExtensionKey), null)
          .withConfigProperty(MarketingCloudConf.DATA_EXTENSION);
        throw collector.getOrThrowException();
      }
      Map<String, String> lowercaseToOriginal = schema.getFields().stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toMap(String::toLowerCase, f -> f));

      for (ETDataExtensionColumn column : columns) {
        String columnName = column.getName();
        String originalName = lowercaseToOriginal.get(columnName.toLowerCase());
        Schema.Field schemaField = schema.getField(originalName);
        if (schemaField == null) {
          if (column.getIsRequired()) {
            collector.addFailure(
              String.format("Data extension '%s' contains a required column '%s' of type '%s' that is not present in " +
                              "the input schema.", dataExtensionKey, columnName, column.getType()), null)
              .withConfigProperty(MarketingCloudConf.DATA_EXTENSION);
          }
        } else {
          Schema fieldSchema = schemaField.getSchema();
          if (fieldSchema.isNullable()) {
            fieldSchema = fieldSchema.getNonNullable();
          }
          Schema.Type schemaType = fieldSchema.getType();
          if (schemaType == Schema.Type.STRING) {
            // Allow strings for anything, as the API just takes strings.
            // If the value is malformed, it will fail at runtime.
            continue;
          }
          String schemaTypeStr = fieldSchema.getLogicalType() != null ?
            fieldSchema.getLogicalType().getToken() : schemaType.name().toLowerCase();
          String fieldName = schemaField.getName();
          switch (column.getType()) {
            case BOOLEAN:
              if (schemaType != Schema.Type.BOOLEAN) {
                collector.addFailure(
                  String.format("Column '%s' is a boolean in data extension '%s', but is a '%s' in the input schema.",
                                originalName, dataExtensionKey, schemaTypeStr),
                  "Change the field schema to ensure it is a boolean or string type.");
              }
              break;
            case DECIMAL:
              if (fieldSchema.getLogicalType() != Schema.LogicalType.DECIMAL) {
                collector.addFailure(
                  String.format("Column '%s' is a decimal in data extension '%s', but is a '%s' in the input schema.",
                                originalName, dataExtensionKey, schemaTypeStr),
                  "Change the field schema to ensure it is a decimal or string type.");
              }
              break;
            case PHONE:
            case TEXT:
            case EMAIL_ADDRESS:
            case LOCALE:
              collector.addFailure(
                String.format("Column '%s' is a %s in data extension '%s', but is a '%s' in the input schema.",
                              originalName, column.getType().name().toLowerCase(), dataExtensionKey, schemaTypeStr),
                "Change the field schema to ensure it is a string type.");
              break;
            case DATE:
              if (fieldSchema.getLogicalType() != Schema.LogicalType.DATE) {
                collector.addFailure(
                  String.format("Column '%s' is a date in data extension '%s', but is a '%s' in the input schema.",
                                originalName, dataExtensionKey, schemaTypeStr),
                  "Change the field schema to ensure it is a date or string type.");
              }
              break;
            case NUMBER:
              if (schemaType != Schema.Type.INT) {
                collector.addFailure(
                  String.format("Column '%s' is a number in data extension '%s', but is a '%s' in the input schema.",
                                originalName, dataExtensionKey, schemaTypeStr),
                  "Change the field schema to ensure it is an integer or string type.");
              }
              break;
            default:
              collector.addFailure(
                String.format("Unknown type '%s' for column '%s' in data extension '%s'.",
                              column.getType(), column.getName(), dataExtensionKey),
                "Supported types are: boolean, decimal, phone, text, email_address, locale, date and number.");
          }
        }
      }
      collector.getOrThrowException();
      return null;
    });
  }

  public DataExtensionInfo getDataExtensionInfo() throws ETSdkException {
    return call(() -> {
      List<ETDataExtensionColumn> columns = ETDataExtension.retrieveColumns(client, dataExtensionKey);
      return new DataExtensionInfo(dataExtensionKey, columns);
    });
  }

  public List<ETDataExtensionRow> scan() throws ETSdkException {
    return call(() -> ETDataExtension.select(client, "key=" + dataExtensionKey).getObjects());
  }

  public ETResponse<ETDataExtensionRow> insert(List<ETDataExtensionRow> rows) throws ETSdkException {
    return call(() -> create(client, rows));
  }

  public ETResponse<ETDataExtensionRow> update(List<ETDataExtensionRow> rows) throws ETSdkException {
    return call(() -> update(client, rows));
  }

  public List<ETResult<ETDataExtensionRow>> upsert(List<ETDataExtensionRow> rows) throws ETSdkException {
    ETResponse<ETDataExtensionRow> inserts = insert(rows);
    List<ETResult<ETDataExtensionRow>> result = new ArrayList<>(rows.size());

    List<ETDataExtensionRow> toUpdate = new ArrayList<>();
    for (ETResult<ETDataExtensionRow> row : inserts.getResults()) {
      // super hacky to check the error message... but there is no better way
      if (row.getStatus() == ETResult.Status.ERROR && row.getErrorCode() == 2 && row.getErrorMessage() != null &&
        row.getErrorMessage().toLowerCase().contains("primary key")) {
        ETDataExtensionRow failed = row.getObject();
        ETDataExtensionRow copy = new ETDataExtensionRow();
        copy.setDataExtensionKey(failed.getDataExtensionKey());
        for (String column : failed.getColumnNames()) {
          copy.setColumn(column, failed.getColumn(column));
        }
        toUpdate.add(copy);
      } else {
        result.add(row);
      }
    }

    if (!toUpdate.isEmpty()) {
      ETResponse<ETDataExtensionRow> updates = update(client, toUpdate);
      result.addAll(updates.getResults());
    }
    return result;
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

  /**
   * Copied from ETSoapObject.create(), except modified to include the error message.
   */
  private static <T extends ETSoapObject> ETResponse<T> create(ETClient client,
                                                               List<T> objects) throws ETSdkException {
    ETResponse<T> response = new ETResponse<>();

    if (objects == null || objects.size() == 0) {
      response.setStatus(ETResult.Status.OK);
      return response;
    }

    //
    // Get handle to the SOAP connection:
    //

    ETSoapConnection connection = client.getSoapConnection();

    //
    // Automatically refresh the token if necessary:
    //

    client.refreshToken();

    //
    // Perform the SOAP create:
    //
    StringBuilder objBuilder = new StringBuilder();

    CreateRequest createRequest = new CreateRequest();
    createRequest.setOptions(new CreateOptions());
    for (T object : objects) {
      object.setClient(client);
      createRequest.getObjects().add(object.toInternal());
      objBuilder.append(object.getClass().getSimpleName().substring(2));
    }
    Soap soap = connection.getSoap("create", objBuilder.toString());

    CreateResponse createResponse = soap.create(createRequest);

    response.setRequestId(createResponse.getRequestID());
    if (createResponse.getOverallStatus().equals("OK")) {
      response.setStatus(ETResult.Status.OK);
    } else if (createResponse.getOverallStatus().equals("Error")) {
      response.setStatus(ETResult.Status.ERROR);
    }
    response.setResponseCode(createResponse.getOverallStatus());
    response.setResponseMessage(createResponse.getOverallStatus());
    for (CreateResult createResult : createResponse.getResults()) {
      //
      // Allocate a new (external) object:
      //

      @SuppressWarnings("unchecked")
      Class<T> externalType = (Class<T>) objects.get(0).getClass();

      T externalObject = null;
      try {
        externalObject = externalType.newInstance();
      } catch (Exception ex) {
        throw new ETSdkException("could not instantiate " + externalType.getName(), ex);
      }

      externalObject.setClient(client);

      //
      // Convert from internal representation:
      //

      // not all SOAP calls return the object though some do..
      APIObject internalObject = createResult.getObject();
      if (internalObject != null) {
        externalObject.fromInternal(createResult.getObject());
      } else {
        // XXX populate fields from the object passed to the call?
        externalObject.setId(Integer.toString(createResult.getNewID()));
      }

      //
      // Add result to the list of results:
      //

      ETResult<T> result = new ETResult<T>();
      if (createResult.getStatusCode().equals("OK")) {
        result.setStatus(ETResult.Status.OK);
      } else if (createResult.getStatusCode().equals("Error")) {
        result.setStatus(ETResult.Status.ERROR);
      }
      result.setResponseCode(createResult.getStatusCode());
      result.setResponseMessage(createResult.getStatusMessage());
      result.setErrorCode(createResult.getErrorCode());

      // this is modified, cast to DataExtensionCreateResult since it has the error message
      if (createResult instanceof DataExtensionCreateResult) {
        DataExtensionCreateResult extCreateResult = (DataExtensionCreateResult) createResult;
        result.setErrorMessage(extCreateResult.getErrorMessage());
      }

      // this is also modified, so we know the object that failed
      //if (result.getResponseCode().equals("OK")) { // XXX?
      result.setObject(externalObject);
      //}
      response.addResult(result);
    }

    return response;
  }

  /**
   * Copy of ETSoapObject.update() except with modifications to set error message and original object.
   */
  public static ETResponse<ETDataExtensionRow> update(ETClient client,
                                                      List<ETDataExtensionRow> objects) throws ETSdkException {
    ETResponse<ETDataExtensionRow> response = new ETResponse<>();

    if (objects == null || objects.size() == 0) {
      response.setStatus(ETResult.Status.OK);
      return response;
    }

    //
    // Get handle to the SOAP connection:
    //

    ETSoapConnection connection = client.getSoapConnection();

    //
    // Automatically refresh the token if necessary:
    //

    client.refreshToken();

    //
    // Perform the SOAP update:
    //
    String obj = "";

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setOptions(new UpdateOptions());
    for (ETDataExtensionRow object : objects) {
      object.setClient(client);
      updateRequest.getObjects().add(object.toInternal());
      obj += object.getClass().getSimpleName().substring(2);
    }
    Soap soap = connection.getSoap("update", obj);

    UpdateResponse updateResponse = soap.update(updateRequest);

    response.setRequestId(updateResponse.getRequestID());
    if (updateResponse.getOverallStatus().equals("OK")) {
      response.setStatus(ETResult.Status.OK);
    } else if (updateResponse.getOverallStatus().equals("Error")) {
      response.setStatus(ETResult.Status.ERROR);
    }
    response.setResponseCode(updateResponse.getOverallStatus());
    response.setResponseMessage(updateResponse.getOverallStatus());
    for (UpdateResult updateResult : updateResponse.getResults()) {
      //
      // Allocate a new (external) object:
      //

      @SuppressWarnings("unchecked")
      Class<ETDataExtensionRow> externalType = (Class<ETDataExtensionRow>) objects.get(0).getClass();

      ETDataExtensionRow externalObject = null;
      try {
        externalObject = externalType.newInstance();
      } catch (Exception ex) {
        throw new ETSdkException("could not instantiate "
                                   + externalType.getName(), ex);
      }

      externalObject.setClient(client);

      //
      // Convert from internal representation:
      //

      // not all SOAP calls return the object though some do..
      APIObject internalObject = updateResult.getObject();
      if (internalObject != null) {
        externalObject.fromInternal(updateResult.getObject());
      } else {
        // XXX populate fields from the object passed to the call?
      }

      //
      // Add result to the list of results:
      //

      ETResult<ETDataExtensionRow> result = new ETResult<>();
      if (updateResult.getStatusCode().equals("OK")) {
        result.setStatus(ETResult.Status.OK);
      } else if (updateResult.getStatusCode().equals("Error")) {
        result.setStatus(ETResult.Status.ERROR);
      }
      result.setResponseCode(updateResult.getStatusCode());
      result.setResponseMessage(updateResult.getStatusMessage());
      result.setErrorCode(updateResult.getErrorCode());


      // this is modified, cast to DataExtensionCreateResult since it has the error message
      if (updateResult instanceof DataExtensionUpdateResult) {
        DataExtensionUpdateResult extUpdateResult = (DataExtensionUpdateResult) updateResult;
        result.setErrorMessage(extUpdateResult.getErrorMessage());
      }

      // this is also modified, so we know the object that failed
      //if (result.getResponseCode().equals("OK")) { // XXX?
      result.setObject(externalObject);
      //}

      if (result.getResponseCode().equals("OK")) { // XXX?
        result.setObject(externalObject);
      }
      response.addResult(result);
    }

    return response;
  }

  /**
   * Used to run the client from the command line. Primarily used for testing purposes.
   * Expects arguments in the form [command] [key] [properties files] where properties file is expected
   * to contain lines like:
   *
   * clientId=...
   * clientSecret=...
   * authEndpoint=...
   * soapEndpoint=...
   */
  public static void main(String[] args) throws Exception {
    String command = args[0];
    String key = args[1];
    String propertiesFile = args[2];
    ETConfiguration conf = new ETConfiguration(propertiesFile);

    DataExtensionClient client = new DataExtensionClient(new ETClient(conf), key);

    if ("describe".equals(command)) {
      Collection<ETDataExtensionColumn> columns = client.getDataExtensionInfo().getColumnList();
      System.out.println(columns.stream().map(c -> c.getName() + " " + c.getType()).collect(Collectors.joining("\n")));
    } else if ("scan".equals(command)) {
      Collection<ETDataExtensionColumn> columns = client.getDataExtensionInfo().getColumnList();
      List<String> columnNames = columns.stream().map(ETDataExtensionColumn::getName).collect(Collectors.toList());
      System.out.println(String.join(", ", columnNames));
      for (ETDataExtensionRow row : client.scan()) {
        List<String> values = new ArrayList<>(columnNames.size());
        for (String columnName : columnNames) {
          values.add(row.getColumn(columnName));
        }
        System.out.println(String.join(", ", values));
      }
    }
  }
}
