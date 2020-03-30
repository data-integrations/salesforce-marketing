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

package io.cdap.plugin.sfmc.source.util;

/**
 * Salesforce constants.
 */
public interface SalesforceConstants {

  /**
   * Salesforce plugin name.
   */
  String PLUGIN_NAME = "SalesforceDataExtension";

  /**
   * Configuration property name used to specify the query mode.
   */
  String PROPERTY_QUERY_MODE = "queryMode";

  /**
   * Configuration property name used to specify the object name
   */
  String PROPERTY_OBJECT_NAME = "objectName";

  /**
   * Configuration property name used to specify data extension key
   */
  String PROPERTY_DATA_EXTENSION_KEY = "dataExtensionKey";

  /**
   * Configuration property name used to specify the list of objects
   */
  String PROPERTY_OBJECT_LIST = "objectList";

  /**
   * Configuration property name used to specify the comma-separated list of data extension keys.
   */
  String PROPERTY_DATA_EXTENSION_KEY_LIST = "dataExtensionKeyList";

  /**
   * Configuration property name used to specify the table name field.
   */
  String PROPERTY_TABLE_NAME_FIELD = "tableNameField";

  /**
   * Configuration property name used to specify client id
   */
  String PROPERTY_CLIENT_ID = "clientId";

  /**
   * Configuration property name used to specify client secret
   */
  String PROPERTY_CLIENT_SECRET = "clientSecret";

  /**
   * Configuration property name used to specify REST API endpoint
   */
  String PROPERTY_API_ENDPOINT = "restEndpoint";

  /**
   * Configuration property name used to specify Auth API endpoint
   */
  String PROPERTY_AUTH_API_ENDPOINT = "authEndpoint";

  /**
   * Configuration property name used to specify SOAP endpoint
   */
  String PROPERTY_SOAP_API_ENDPOINT = "soapEndpoint";

  String TABLE_PREFIX = "multisink.";

  int MAX_PAGE_SIZE = 2500;
}
