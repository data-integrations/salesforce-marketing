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
import io.cdap.plugin.sfmc.source.MarketingCloudInputSplit;
import io.cdap.plugin.sfmc.source.MarketingCloudSource;
import io.cdap.plugin.sfmc.source.MarketingCloudSourceConfig;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
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

    private StructuredRecord record;
    private Iterator<? extends ETApiObject> iterator;

    private List<? extends ETApiObject> results;

    private String requestId;

    private String dataExtensionKey = "";


    @Override
    public void test(ConnectorContext connectorContext) throws ValidationException {
        FailureCollector collector = connectorContext.getFailureCollector();
        config.validateCredentials(collector);
    }

    @Override
    public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
        BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
        int count = 0;
        try {
            for (SourceObject object : SourceObject.values()) {
                String name = object.getValue();
                BrowseEntity.Builder entity = (BrowseEntity.builder(String.valueOf(object), String.valueOf(object),
                                ENTITY_TYPE_OBJECTS).
                        canBrowse(false).canSample(true));
                browseDetailBuilder.addEntity(entity.build());
                count++;
            }
        } catch (Exception e) {
            throw new IOException("Unable to create a Connection", e);
        }
        return browseDetailBuilder.setTotalCount(count).build();
    }

    @Override
    public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
            throws IOException {
        ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
        properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
        String tableName = connectorSpecRequest.getPath();
        if (tableName != null) {
            properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(tableName));
            properties.put(MarketingCloudConstants.PROPERTY_OBJECT_NAME, SourceObject.valueOf(tableName).getValue());
            schema = config.getSchema(SourceObject.valueOf(tableName));
            specBuilder.setSchema(schema);
        }
        return specBuilder.addRelatedPlugin(new PluginSpec(MarketingCloudConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
                properties)).addRelatedPlugin(new PluginSpec(MarketingCloudDataExtensionSink.PLUGIN_TYPE,
                BatchSink.PLUGIN_TYPE, properties)).build();
    }

    @Override
    public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
            throws IOException {
        String object = sampleRequest.getPath();
        if (object == null) {
            throw new IllegalArgumentException("Path should contain object");
        }
        try {
            return listObjectDetails(SourceObject.valueOf(object));
        } catch (Exception e) {
            throw new IOException("unable to fetch records", e);
        }
    }

    private List<StructuredRecord> listObjectDetails(SourceObject sourceObject) throws ETSdkException
            , IOException {
        List<StructuredRecord> sample = new ArrayList<>();
        MarketingCloudClient marketingCloudClient = MarketingCloudClient.create
                (config.getClientId(), config.getClientSecret(),
                        config.getAuthEndpoint(), config.getSoapEndpoint());
        if (sourceObject == SourceObject.DATA_EXTENSION) {
            return Collections.emptyList();
        } else {
            response = marketingCloudClient.fetchObjectRecords(sourceObject, null);
            results = response.getObjects();
            iterator = results.iterator();
            while (iterator.hasNext()) {
                StructuredRecord.Builder builder = StructuredRecord.builder(schema);
                row = iterator.next();
                config.convertRecord(sourceObject, builder, row);
                StructuredRecord structuredRecord = builder.build();
                sample.add(structuredRecord);
            }
        }
        return sample;
    }
}
