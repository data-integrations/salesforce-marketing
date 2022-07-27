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

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.sfmc.common.SalesforceMarketingCloudBaseConfig;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import io.cdap.plugin.sfmc.source.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Configuration for the {@link MarketingCloudSource}.
 */
public class MarketingCloudSourceConfig extends SalesforceMarketingCloudBaseConfig {
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudSourceConfig.class);

  @Name(MarketingCloudConstants.PROPERTY_QUERY_MODE)
  @Macro
  @Description("Mode of data retrieval. The mode can be one of two values: "
    + "`Multi Object` - will allow user to fetch data for multiple data extensions, "
    + "`Single Object` - will allow user to fetch data for single data extension.")
  private String queryMode;

  @Name(MarketingCloudConstants.PROPERTY_OBJECT_NAME)
  @Macro
  @Nullable
  @Description("Specify the object for which data to be fetched. This can be one of following values: " +
    "`Data Extension` - will allow user to fetch data for a single Data Extension object, " +
    "`Campaign` - will allow user to fetch data for Campaign object, " +
    "`Email` - will allow user to fetch data for Email object, " +
    "`Mailing List` - will allow user to fetch data for Mailing List object. " +
    "Note, this value will be ignored if the Mode is set to `Multi Object`.")
  private String objectName;

  @Name(MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY)
  @Macro
  @Nullable
  @Description("Specify the data extension key from which data to be fetched. Note, this value will be ignored in " +
    "following two cases: 1. If the Mode is set to `Multi Object`, 2. If the selected object name is other than " +
    "`Data Extension`.")
  private String dataExtensionKey;

  @Name(MarketingCloudConstants.PROPERTY_OBJECT_LIST)
  @Macro
  @Nullable
  @Description("Specify the comma-separated list of objects for which data to be fetched; for example: " +
    "'Object1,Object2'. This can be one or more values from following possible values: " +
    "`Data Extension` - will allow user to fetch data for a single Data Extension object, " +
    "`Campaign` - will allow user to fetch data for Campaign object, " +
    "`Email` - will allow user to fetch data for Email object, " +
    "`Mailing List` - will allow user to fetch data for Mailing List object. " +
    "Note, this value will be ignored if the Mode is set to `Single Object`.")
  private String objectList;

  @Name(MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY_LIST)
  @Macro
  @Nullable
  @Description("Specify the data extension keys from which data to be fetched; for example: 'Key1,Key2'. " +
    "Note, this value will be ignored in following two cases: 1. If the Mode is set to `Single Object`, " +
    "2. If the selected object list does not contain `Data Extension` as one of the objects.")
  private String dataExtensionKeys;

  @Name(MarketingCloudConstants.PROPERTY_TABLE_NAME_FIELD)
  @Macro
  @Nullable
  @Description("The name of the field that holds the object name to which the data belongs to. Must not be the name " +
    "of any column for any of the objects that will be read. Defaults to `tablename`. In case of `Data Extension` " +
    "object, this field will have value in `dataextension_[Data Extension Key]` format. Note, the Table name field " +
    "value will be ignored if the Mode is set to `Single Object`.")
  private String tableNameField;

  @Name(MarketingCloudConstants.PROPERTY_FILTER)
  @Macro
  @Nullable
  @Description("The WHERE clause used to filter data from Marketing cloud objects.")
  private String filter;

  /**
   * Constructor for MarketingCloudSourceConfig object.
   *
   * @param referenceName     The reference name
   * @param queryMode         The query mode
   * @param objectName        The object name to be fetched from Salesforce Marketing Cloud
   * @param dataExtensionKey  The data extension key to be fetched from Salesforce Marketing Cloud
   * @param objectList        The list of objects to be fetched from Salesforce Marketing Cloud
   * @param dataExtensionKeys The list of data extension keys to be fetched from Salesforce Marketing Cloud
   * @param tableNameField    The field name to hold the table name value
   * @param clientId          The Salesforce Marketing Cloud Client Id
   * @param clientSecret      The Salesforce Marketing Cloud Client Secret
   * @param authEndpoint      The AUTH API endpoint for Salesforce Marketing Cloud
   * @param soapEndpoint      The SOAP API endpoint for Salesforce Marketing Cloud
   */
  public MarketingCloudSourceConfig(String referenceName, String queryMode, @Nullable String objectName,
                                    @Nullable String dataExtensionKey, @Nullable String objectList,
                                    @Nullable String dataExtensionKeys, @Nullable String tableNameField,
                                    @Nullable String filter, String clientId, String clientSecret,
                                    String authEndpoint, String soapEndpoint) {
    super(referenceName, clientId, clientSecret, authEndpoint, soapEndpoint);
    this.queryMode = queryMode;
    this.objectName = objectName;
    this.dataExtensionKey = dataExtensionKey;
    this.objectList = objectList;
    this.dataExtensionKeys = dataExtensionKeys;
    this.tableNameField = tableNameField;
    this.filter = filter;

  }

  /**
   * Returns the query mode chosen.
   *
   * @param collector The failure collector to collect the errors
   * @return An instance of SourceQueryMode
   */
  public SourceQueryMode getQueryMode(FailureCollector collector) {
    SourceQueryMode mode = getQueryMode();
    if (mode != null) {
      return mode;
    }

    collector.addFailure("Unsupported query mode value: " + queryMode,
                         String.format("Supported modes are: %s", SourceQueryMode.getSupportedModes()))
      .withConfigProperty(MarketingCloudConstants.PROPERTY_QUERY_MODE);
    collector.getOrThrowException();
    return null;
  }

  /**
   * Returns the query mode chosen.
   *
   * @return An instance of SourceQueryMode
   */
  public SourceQueryMode getQueryMode() {
    Optional<SourceQueryMode> sourceQueryMode = SourceQueryMode.fromValue(queryMode);

    return sourceQueryMode.isPresent() ? sourceQueryMode.get() : null;
  }

  /**
   * Returns selected object.
   *
   * @param collector The failure collector to collect the errors
   * @return An instance of SourceObject
   */
  public SourceObject getObject(FailureCollector collector) {
    SourceObject sourceObject = getObject();
    if (sourceObject != null) {
      return sourceObject;
    }

    collector.addFailure("Unsupported object value: " + objectName,
                         String.format("Supported objects are: %s", SourceObject.getSupportedObjects()))
      .withConfigProperty(MarketingCloudConstants.PROPERTY_OBJECT_NAME);
    collector.getOrThrowException();
    return null;
  }

  @Nullable
  public SourceObject getObject() {
    return getSourceObject(objectName, filter);
  }

  @Nullable
  public String getDataExtensionKey() {
    return dataExtensionKey;
  }

  /**
   * Returns list of selected objects.
   *
   * @param collector The failure collector to collect the errors
   * @return The list of SourceObject
   */
  public List<SourceObject> getObjectList(FailureCollector collector) {
    List<String> objects = Util.splitToList(objectList, ',');
    List<SourceObject> sourceObjects = new ArrayList<>();

    for (String object : objects) {
      SourceObject sourceObject = getSourceObject(object, filter);
      if (sourceObject == null) {
        collector.addFailure("Unsupported object value: " + object,
                             String.format("Supported objects are: %s", SourceObject.getSupportedObjects()))
          .withConfigProperty(MarketingCloudConstants.PROPERTY_OBJECT_LIST);
        break;
      }
      sourceObjects.add(sourceObject);
    }

    return sourceObjects;
  }

  /**
   * Returns list of selected objects.
   *
   * @return The list of SourceObject
   */
  @Nullable
  public List<SourceObject> getObjectList() {
    List<String> objects = Util.splitToList(objectList, ',');
    List<SourceObject> sourceObjects = new ArrayList<>();

    for (String object : objects) {
      SourceObject sourceObject = getSourceObject(object, filter);
      if (sourceObject == null) {
        continue;
      }
      sourceObjects.add(sourceObject);
    }

    return sourceObjects;
  }

  @Nullable
  public String getDataExtensionKeys() {
    return dataExtensionKeys;
  }

  @Nullable
  public String getTableNameField() {
    return tableNameField;
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  /**
   * Validates {@link MarketingCloudSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    validateCredentials(collector);
    validateQueryMode(collector);
    validateFilter(collector);
  }

  private SourceObject getSourceObject(String objectName, String filter) {
    Optional<SourceObject> sourceObject = SourceObject.fromValue(objectName);
    if (sourceObject.isPresent()) {
      SourceObject obj = sourceObject.get();
      obj.setFilter(filter);
      return obj;
    } else {
      return null;
    }
  }

  private void validateCredentials(FailureCollector collector) {

    if (getConnection() != null) {
      this.getConnection().validateCredentials(collector);
    }
  }

  private void validateQueryMode(FailureCollector collector) {
    //according to query mode check if either object name / object list exists or not
    if (containsMacro(MarketingCloudConstants.PROPERTY_QUERY_MODE)) {
      return;
    }

    SourceQueryMode mode = getQueryMode(collector);

    if (mode == SourceQueryMode.MULTI_OBJECT) {
      validateMultiObjectQueryMode(collector);
    } else {
      validateSingleObjectQueryMode(collector);
    }
  }

  private void validateMultiObjectQueryMode(FailureCollector collector) {
    if (containsMacro(MarketingCloudConstants.PROPERTY_OBJECT_LIST)
      || containsMacro(MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY_LIST)
      || containsMacro(MarketingCloudConstants.PROPERTY_TABLE_NAME_FIELD)) {
      return;
    }

    List<SourceObject> objects = getObjectList(collector);
    collector.getOrThrowException();

    if (objects.isEmpty()) {
      collector.addFailure("At least 1 Object must be specified.", null)
        .withConfigProperty(MarketingCloudConstants.PROPERTY_OBJECT_LIST);
    }

    if (objects.contains(SourceObject.DATA_EXTENSION)) {
      List<String> dataExtensionKeyList = Util.splitToList(getDataExtensionKeys(), ',');
      if (dataExtensionKeyList.isEmpty()) {
        collector.addFailure("At least 1 Data Extension Key must be specified.", null)
          .withConfigProperty(MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY_LIST);
      }
    }

    if (Util.isNullOrEmpty(tableNameField)) {
      collector.addFailure("Table name field must be specified.", null)
        .withConfigProperty(MarketingCloudConstants.PROPERTY_TABLE_NAME_FIELD);
    }
  }

  private void validateSingleObjectQueryMode(FailureCollector collector) {
    if (containsMacro(MarketingCloudConstants.PROPERTY_OBJECT_NAME)
      || containsMacro(MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY)) {
      return;
    }

    SourceObject object = getObject(collector);

    if (object == SourceObject.DATA_EXTENSION && Util.isNullOrEmpty(dataExtensionKey)) {
      collector.addFailure("Data Extension Key must be specified.", null)
        .withConfigProperty(MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY);
    }
  }

  private void validateFilter(FailureCollector collector) {
    if (containsMacro(MarketingCloudConstants.PROPERTY_FILTER) || Util.isNullOrEmpty(filter)) {
      return;
    }
    try {
      MarketingCloudClient.validateFilter(filter);
    } catch (ETSdkException e) {
      collector.addFailure("Filter string is not valid.",
                           "Check syntax to confirm.")
        .withConfigProperty(MarketingCloudConstants.PROPERTY_FILTER)
        .withStacktrace(e.getStackTrace());
    }
  }
}
