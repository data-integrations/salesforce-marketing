/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.sfmc.connector;

import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETDataExtension;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETSoapObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.sfmc.sink.MarketingCloudDataExtensionSink;
import io.cdap.plugin.sfmc.source.MarketingCloudClient;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.MarketingCloudUtil;
import io.cdap.plugin.sfmc.source.util.SourceObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Salesforce Marketing Cloud Connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(MarketingCloudConstants.PLUGIN_NAME)
@Description("Connection to access data from Salesforce Marketing Objects.")
public class MarketingCloudConnector implements DirectConnector {
  private static final String ENTITY_TYPE_OBJECTS = "object";
  private static final String LABEL_NAME = "Customer Key";
  private final MarketingConnectorConfig config;

  public ETApiObject row;
  private ETResponse<? extends ETSoapObject> response;
  private Schema schema;
  private Iterator<? extends ETApiObject> iterator;

  MarketingCloudConnector(MarketingConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validateCredentials(collector);
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) {
    config.validateCredentials(connectorContext.getFailureCollector());
    if (browseRequest.getPath().equals(SourceObject.DATA_EXTENSION.toString())) {
      return dataExtensionKeyList();
    }
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    for (SourceObject object : SourceObject.values()) {
      if (object == SourceObject.DATA_EXTENSION) {
        BrowseEntity.Builder entity = (BrowseEntity.builder(String.valueOf(object), String.valueOf(object),
                                                            ENTITY_TYPE_OBJECTS).canBrowse(true).canSample(false));
        browseDetailBuilder.addEntity(entity.build());
      } else {
        BrowseEntity.Builder entity = (BrowseEntity.builder(String.valueOf(object), String.valueOf(object),
                                                            ENTITY_TYPE_OBJECTS).canBrowse(false).canSample(true));
        browseDetailBuilder.addEntity(entity.build());
      }
    }
    return browseDetailBuilder.setTotalCount(SourceObject.values().length).build();
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest) {
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> properties = new HashMap<>();
    properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    String objectName = connectorSpecRequest.getPath();
    MarketingCloudClient marketingCloudClient = null;
    try {
      marketingCloudClient = MarketingCloudClient.getOrCreate(config.getClientId(),
                                                              config.getClientSecret(), config.getAuthEndpoint(),
                                                              config.getSoapEndpoint());
    } catch (ETSdkException e) {
      throw new RuntimeException(e);
    }
    if (objectName != null && !objectName.startsWith(SourceObject.DATA_EXTENSION.toString())) {
      properties.put(MarketingCloudConstants.PROPERTY_OBJECT_NAME, SourceObject.valueOf(objectName).getValue());
      try {
        schema = config.getSchema(SourceObject.valueOf(objectName));
      } catch (ETSdkException e) {
        throw new RuntimeException(e);
      }
    } else {
      objectName = objectName.substring(objectName.indexOf("/") + 1);
      properties.put(MarketingCloudConstants.PROPERTY_OBJECT_NAME, SourceObject.DATA_EXTENSION.getValue());
      properties.put(MarketingCloudConstants.PROPERTY_DATA_EXTENSION_KEY, objectName);
      try {
        schema = marketingCloudClient.fetchDataExtensionSchema(objectName).getSchema();
      } catch (ETSdkException e) {
        throw new RuntimeException(e);
      }
    }
    properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(objectName));
    specBuilder.setSchema(schema);
    return specBuilder.addRelatedPlugin(new PluginSpec(MarketingCloudConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
                                                       properties))
      .addRelatedPlugin(new PluginSpec(MarketingCloudDataExtensionSink.PLUGIN_TYPE,
                                       BatchSink.PLUGIN_TYPE, properties)).build();
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
    throws IOException {
    try {
      MarketingCloudUtil cloudUtil = new MarketingCloudUtil();
      MarketingCloudClient marketingCloudClient = MarketingCloudClient.getOrCreate(config.getClientId(),
                                                                                   config.getClientSecret(),
                                                                                   config.getAuthEndpoint(),
                                                                                   config.getSoapEndpoint());
      String objectName = sampleRequest.getPath();
      if (objectName == null) {
        throw new IllegalArgumentException("Path should not be null.");
      }
      if (objectName.startsWith(SourceObject.DATA_EXTENSION.toString())) {
        String dataExtensionKey = objectName.substring(SourceObject.DATA_EXTENSION.toString().length() + 1);
        return dataExtensionRecords(dataExtensionKey, cloudUtil, marketingCloudClient);
      }
      return listObjectDetails(SourceObject.valueOf(objectName), cloudUtil, marketingCloudClient);
    } catch (ETSdkException e) {
      throw new IOException("unable to fetch records", e);
    }
  }

  /**
   * returns the records for the DATA_EXTENSION Object
   */
  private List<StructuredRecord> dataExtensionRecords(String dataExtensionKey, MarketingCloudUtil cloudUtil,
                                                      MarketingCloudClient marketingCloudClient)
    throws ETSdkException {
    List<StructuredRecord> sampleList = new ArrayList<>();
    response = marketingCloudClient.fetchDataExtensionRecords(dataExtensionKey, null, null);
    MarketingCloudObjectInfo sfObjectMetaData = marketingCloudClient.fetchDataExtensionSchema(dataExtensionKey);
    iterator = response.getObjects().iterator();
    while (iterator.hasNext()) {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      row = iterator.next();
      MarketingCloudUtil.convertRecord(sfObjectMetaData, builder, row);
      sampleList.add(builder.build());
    }
    return sampleList;
  }

  /**
   * returns the records for the Object.
   */

  private List<StructuredRecord> listObjectDetails(SourceObject sourceObject, MarketingCloudUtil cloudUtil,
                                                   MarketingCloudClient marketingCloudClient)
    throws ETSdkException {
    List<StructuredRecord> sampleList = new ArrayList<>();
    response = marketingCloudClient.fetchObjectRecords(sourceObject, null, null);
    schema = config.getSchema(sourceObject);
    MarketingCloudObjectInfo sfObjectMetaData = marketingCloudClient.fetchObjectSchema(sourceObject);
    iterator = response.getObjects().iterator();
    while (iterator.hasNext()) {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      row = iterator.next();
      MarketingCloudUtil.convertRecord(sfObjectMetaData, builder, row);
      sampleList.add(builder.build());
    }
    return sampleList;
  }

  public BrowseDetail dataExtensionKeyList() {
    try {
      MarketingCloudClient marketingCloudClient = MarketingCloudClient.getOrCreate(config.getClientId(),
                                                                                   config.getClientSecret(),
                                                                                   config.getAuthEndpoint(),
                                                                                   config.getSoapEndpoint());
      ETResponse<ETDataExtension> response = marketingCloudClient.retrieveDataExtensionKeys();
      Map<String, String> map = response.getObjects().stream().collect(Collectors.toMap(ETDataExtension::getKey,
                                                                                        ETDataExtension::getName));
      BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
      for (ETDataExtension key : response.getObjects()) {
        BrowseEntity.Builder entity = (BrowseEntity.builder(key.getName(),
                                                            SourceObject.DATA_EXTENSION + "/" + key.getKey(),
                                                            SourceObject.DATA_EXTENSION.getValue()).canBrowse(false)
          .canSample(true));
        entity.addProperty(LABEL_NAME, BrowseEntityPropertyValue.builder(key.getKey(), BrowseEntityPropertyValue.
          PropertyType.STRING).build());
        browseDetailBuilder.addEntity(entity.build());
      }
      return browseDetailBuilder.setTotalCount(map.size()).build();
    } catch (ETSdkException e) {
      throw new RuntimeException(e);
    }
  }
}

