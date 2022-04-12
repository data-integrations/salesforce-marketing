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

package com.custom.fuelsdk;

import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETDataExtensionColumn;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETExpression;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETResult;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETSoapConnection;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.annotations.SoapObject;
import com.exacttarget.fuelsdk.internal.APIObject;
import com.exacttarget.fuelsdk.internal.RetrieveRequest;
import com.exacttarget.fuelsdk.internal.RetrieveRequestMsg;
import com.exacttarget.fuelsdk.internal.RetrieveResponseMsg;
import com.exacttarget.fuelsdk.internal.Soap;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.exacttarget.fuelsdk.ETDataExtension.retrieveColumns;

/**
 * Extending class to fix the bug with Fuel-SDK on pagination.
 * SFMC SOAP API exposes API's that can retrieve a max of 2500 records.
 * if there are more than 2500 records then the api returns
 * back an indicator 'MoreDataAvailable' so the original requester
 * can issue another call by passing the RequestID to retrive
 * the next 2500 elements. If all elements are retrived then the
 * final batch response will not have the 'MoreDataAvailable'
 * ETSoapObject exposes a method retrieve that should support this
 * pagination but using the method there is a bug .
 * By passing the requestID( next bactch) the method fails as the
 * SOAP client is not properly initialized.
 *
 * The fix was to extend this object and copy the retrieve method
 * to customRetrieve and properly initialize the SOAP client when
 * the request contained the RequestID to retrive the next batch
 * of records.
 *
 */
public class PaginationETSoapObject extends ETSoapObject {

  static final int DEFAULT_PAGE_SIZE = 2500;
  private static Logger logger = Logger.getLogger(PaginationETSoapObject.class);

  /**
   * @param client
   * @param dataExtension
   * @param filter
   * @return
   * @throws ETSdkException
   *
   * This method is from ETDataExtension with fix for pagination
   * of Data Extension object.
   */
  public static ETResponse<ETDataExtensionRow> select(ETClient client,
                                                      String dataExtension,
                                                      ETFilter filter)
    throws ETSdkException {
    String name = null;

    //
    // The data extension can be specified using key or name:
    //

    ETExpression e = ETExpression.parse(dataExtension);
    if (e.getProperty().toLowerCase().equals("key")
      && e.getOperator() == ETExpression.Operator.EQUALS) {
      name = e.getValue();
      // if no columns are explicitly requested
      // retrieve all columns
      if (filter.getProperties().isEmpty()) {
        filter.setProperties(retrieveColumnNames(client, name));
      }
    } else if (e.getProperty().toLowerCase().equals("name")
      && e.getOperator() == ETExpression.Operator.EQUALS) {
      name = e.getValue();
      // if no columns are explicitly requested
      // throw an exception
      // because we need the key
      // to retrieve columns
      if (filter.getProperties().isEmpty()) {
        throw new ETSdkException("columns must be specified "
                                   + "when retrieving data extensions by name");
      }

    } else {
      throw new ETSdkException("invalid data extension filter string");
    }

    /*
    The original code which is commented now

    ETResponse<ETDataExtensionRow> response =
                ETSoapObject.retrieve(client,
                                      "DataExtensionObject[" + name + "]",
                                      filter,
                                      ETDataExtensionRow.class);

        List<ETResult<ETDataExtensionRow>> rowSet = sortRowSet(response.getResults(), filter);

        List<ETResult<ETDataExtensionRow>> paginatedRowSet;
           if (page == null) {
              page = response.getPage();
               if (page == null) {
                 page = 1;
             }
         }
         int iPage = page;
         if (pageSize == null) {
             pageSize = response.getPageSize();
             if (pageSize == null) {
                 pageSize = DEFAULT_PAGE_SIZE;
             }
         }
         int iPageSize = pageSize;
         int pageStart = (iPage - 1) * iPageSize;
         int pageEnd = pageStart + iPageSize;
         if (pageStart < rowSet.size()) {
             if (pageEnd > rowSet.size()) {
                 pageEnd = rowSet.size();
             }
         } else {
             pageEnd = pageStart;
         }
         paginatedRowSet = rowSet.subList(pageStart, pageEnd);
         response = createResponse(response, paginatedRowSet, page, pageSize, pageEnd, rowSet.size());
         logger.debug("final response: " + response);

      This code is commented out and replaced with customRetrieve shown below.

     */

    /*Start BUG FIX*/
    ETResponse<ETDataExtensionRow> response = customRetrieve(client, "DataExtensionObject[" + name + "]",
                                                             filter, null, ETDataExtensionRow.class);
    /*End BUG FIX*/
    return response;
  }

  /**
   * @param client
   * @param soapObject
   * @param requestID
   * @param filter
   * @return
   * @throws ETSdkException
   */
  public static ETResponse<ETDataExtensionRow> continueRequest(ETClient client, String soapObject, String requestID,
                                                               ETFilter filter) throws ETSdkException {



    ETResponse<ETDataExtensionRow> response = customRetrieve(client, soapObject, filter, requestID,
                                                             ETDataExtensionRow.class);
    return response;
  }

  /*
  This method was copied from class ETSoapObject method retreive and modified to fix
  the bug for pagination.
  */

  public static <T extends ETSoapObject> ETResponse<T> customRetrieve(ETClient client,
                                                                      String soapObjectName,
                                                                      ETFilter filter,
                                                                      String continueRequest,
                                                                      Class<T> type)
    throws ETSdkException {
    ETResponse<T> response = new ETResponse<T>();

    //
    // Get handle to the SOAP connection:
    //

    ETSoapConnection connection = client.getSoapConnection();

    //
    // Automatically refresh the token if necessary:
    //

    client.refreshToken();

    //
    // Read internal type from the SoapObject annotation:
    //

    Class<T> externalType = type; // for code readability

    SoapObject internalTypeAnnotation
      = externalType.getAnnotation(SoapObject.class);
    assert internalTypeAnnotation != null;
    Class<? extends APIObject> internalType = internalTypeAnnotation.internalType();
    assert internalType != null;

    ETExpression expression = filter.getExpression();

    //
    // Determine properties to retrieve:
    //

    List<String> externalProperties = filter.getProperties();
    List<String> internalProperties = null;

    if (externalProperties.size() > 0) {
      //
      // Only request those properties specified:
      //

      internalProperties = new ArrayList<String>();

      for (String externalProperty : externalProperties) {
        String internalProperty =
          getInternalProperty(externalType, externalProperty);
        assert internalProperty != null;
        internalProperties.add(internalProperty);
      }
    } else {
      //
      // No properties were explicitly requested:
      //

      internalProperties = getInternalProperties(externalType);

      //
      // Remove properties that are unretrievable:
      //

      for (String property : internalTypeAnnotation.unretrievable()) {
        internalProperties.remove(property);
      }
    }

    //
    // remove subscribers field from List object as subscribers is not a retrieval field.
    //
    if (SourceObject.MAILING_LIST.getClassRef().equals(type)) {
      internalProperties.remove(MarketingCloudConstants.SUBSCRIBER);
    }
    //
    // Perform the SOAP retrieve:
    //

    //Soap soap = connection.getSoap();
    Soap soap = null;

    RetrieveRequest retrieveRequest = new RetrieveRequest();

    if (continueRequest == null) {
      // if soapObjectType is specified, use it; otherwise, default
      // to the name of the internal class representing the object:
      if (soapObjectName != null) {
        retrieveRequest.setObjectType(soapObjectName);
        soap = connection.getSoap("retrieve", soapObjectName);
      } else {
        retrieveRequest.setObjectType(internalType.getSimpleName());
        soap = connection.getSoap("retrieve", internalType.getSimpleName());
      }
      retrieveRequest.getProperties().addAll(internalProperties);

      if (expression.getOperator() != null) {
        //
        // Convert the property names to their internal counterparts:
        //

        String property = expression.getProperty();
        if (property != null) {
          expression.setProperty(getInternalProperty(type, property));
        }
        for (ETExpression subexpression : expression.getSubexpressions()) {
          String p = subexpression.getProperty();
          if (p != null) {
            subexpression.setProperty(getInternalProperty(type, p));
          }
        }

        retrieveRequest.setFilter(toFilterPart(expression));
      }
    } else {
      if (continueRequest != null) {
        retrieveRequest.setContinueRequest(continueRequest);
      }
    }

    if (logger.isTraceEnabled()) {
      logger.trace("RetrieveRequest:");
      logger.trace("  objectType = " + retrieveRequest.getObjectType());
      StringBuilder line = new StringBuilder("  properties = { ");
      //String line = null;
      for (String property : retrieveRequest.getProperties()) {

        line.append(property).append(", ");
      }
      if (retrieveRequest.getProperties().size() > 0) {
        line.setLength(line.length() - 2);
      }
      line.append(" }");
      logger.trace(line.toString());
      if (filter != null) {
        logger.trace("  filter = " + toFilterPart(expression));
      }
    }

    logger.trace("calling soap.retrieve...");

    RetrieveRequestMsg retrieveRequestMsg = new RetrieveRequestMsg();
    retrieveRequestMsg.setRetrieveRequest(retrieveRequest);

    /*Start BUG FIX*/
    if (soap == null) {
      if (soapObjectName != null) {
        soap = connection.getSoap("retrieve", soapObjectName);
      } else {
        soap = connection.getSoap("retrieve", internalType.getSimpleName());
      }
    }
    /*End BUG FIX*/
    RetrieveResponseMsg retrieveResponseMsg = soap.retrieve(retrieveRequestMsg);

    if (logger.isTraceEnabled()) {
      logger.trace("RetrieveResponseMsg:");
      logger.trace("  requestId = " + retrieveResponseMsg.getRequestID());
      logger.trace("  overallStatus = " + retrieveResponseMsg.getOverallStatus());
      logger.trace("  results = {");
      for (APIObject result : retrieveResponseMsg.getResults()) {
        logger.trace("    " + result);
      }
      logger.trace("  }");
    }

    response.setRequestId(retrieveResponseMsg.getRequestID());
    if (retrieveResponseMsg.getOverallStatus().equals("OK")) {
      response.setStatus(ETResult.Status.OK);
    } else if (retrieveResponseMsg.getOverallStatus().equals("Error")) {
      response.setStatus(ETResult.Status.ERROR);
    }
    response.setResponseCode(retrieveResponseMsg.getOverallStatus());
    response.setResponseMessage(retrieveResponseMsg.getOverallStatus());
    for (APIObject internalObject : retrieveResponseMsg.getResults()) {
      //
      // Allocate a new (external) object:
      //

      T externalObject = null;
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

      externalObject.fromInternal(internalObject);

      //
      // Add result to the list of results:
      //

      ETResult<T> result = new ETResult<T>();
      result.setObject(externalObject);
      response.addResult(result);
    }

    if (retrieveResponseMsg.getOverallStatus().equals("MoreDataAvailable")) {
      response.setMoreResults(true);
    }

    return response;
  }

  /**
   * @param client The ETClient object
   * @param key    The key
   * @return The List of Column names
   * @throws ETSdkException
   */
  private static List<String> retrieveColumnNames(ETClient client,
                                                  String key)
    throws ETSdkException {
    return getColumnNames(retrieveColumns(client, key));
  }


  /**
   * @param columns The List of ETDataExtensionColumn object
   * @return The List of ETDataExtension Column names
   */
  private static List<String> getColumnNames(List<ETDataExtensionColumn> columns) {
    List<String> columnNames = new ArrayList<String>();
    for (ETDataExtensionColumn column : columns) {
      columnNames.add(column.getName());
    }
    return columnNames;
  }

  @Override
  public String getId() {
    return null;
  }

  @Override
  public void setId(String s) {

  }
}
