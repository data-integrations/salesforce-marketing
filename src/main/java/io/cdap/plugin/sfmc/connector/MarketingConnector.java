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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Salesforce Marketing Cloud Connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(MarketingCloudConstants.PLUGIN_NAME)
@Description("Connection to access data from Salesforce Marketing Objects.")

public class MarketingConnector implements DirectConnector {
  private MarketingConnectorConfig config;
  private static final String ENTITY_TYPE_OBJECTS = "object";
  private ETResponse<? extends ETSoapObject> response;
  private Schema schema;

  public ETApiObject row;
  private Iterator<? extends ETApiObject> iterator;

  MarketingConnector (MarketingConnectorConfig config) {
  this.config = config;
  }

  @Override
  public void test (ConnectorContext connectorContext) throws ValidationException {
  FailureCollector collector = connectorContext.getFailureCollector();
  config.validateCredentials(collector);
  }

  @Override
  public BrowseDetail browse (ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
  BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
  int count = 0;
      for (SourceObject object : SourceObject.values()) {
        BrowseEntity.Builder entity = (BrowseEntity.builder(String.valueOf(object), String.valueOf(object),
        ENTITY_TYPE_OBJECTS).canBrowse(false).canSample(true));
        browseDetailBuilder.addEntity(entity.build());
        count++;
      }
  return browseDetailBuilder.setTotalCount(count).build();
  }

  @Override
  public ConnectorSpec generateSpec (ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
  throws IOException {
  ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
  Map<String, String> properties = new HashMap<>();
  properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
  properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
  String objectName = connectorSpecRequest.getPath();
  if (objectName != null) {
      properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(objectName));
      properties.put(MarketingCloudConstants.PROPERTY_OBJECT_NAME, SourceObject.valueOf(objectName).getValue());
      schema = config.getSchema(SourceObject.valueOf(objectName));
      specBuilder.setSchema(schema);
    }
  return specBuilder.addRelatedPlugin(new PluginSpec(MarketingCloudConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
          properties)).addRelatedPlugin(new PluginSpec(MarketingCloudDataExtensionSink.PLUGIN_TYPE,
          BatchSink.PLUGIN_TYPE, properties)).build();
  }

  @Override
  public List<StructuredRecord> sample (ConnectorContext connectorContext, SampleRequest sampleRequest)
          throws IOException {
  String objectName = sampleRequest.getPath();
  if (objectName == null) {
      throw new IllegalArgumentException("Path should contain object name");
  }
  try {
      return listObjectDetails(SourceObject.valueOf(objectName));
  } catch (ETSdkException e) {
      throw new IOException("unable to fetch records", e);
  }
  }

  private List<StructuredRecord> listObjectDetails (SourceObject sourceObject) throws ETSdkException
          , IOException {
  List<StructuredRecord> sampleList = new ArrayList<>();
  MarketingCloudUtil cloudUtil = new MarketingCloudUtil();
  MarketingCloudClient marketingCloudClient = MarketingCloudClient.create
          (config.getClientId(), config.getClientSecret(), config.getAuthEndpoint(), config.getSoapEndpoint());
  //  returning Collections.emptyList for DATA_EXTENSION Source Object because we need to provide requestId which
  //  is dynamic. However, provided requestId null.
  if (SourceObject.DATA_EXTENSION == sourceObject) {
      return Collections.emptyList();
  } else {
      response = marketingCloudClient.fetchObjectRecords(sourceObject, null);
      MarketingCloudObjectInfo sfObjectMetaData = marketingCloudClient.fetchObjectSchema(sourceObject);
      iterator = response.getObjects().iterator();
      while (iterator.hasNext()) {
          StructuredRecord.Builder builder = StructuredRecord.builder(schema);
          row = iterator.next();
          cloudUtil.convertRecord(sfObjectMetaData, builder, row);
          sampleList.add(builder.build());
      }
  }
  return sampleList;
  }
 }
