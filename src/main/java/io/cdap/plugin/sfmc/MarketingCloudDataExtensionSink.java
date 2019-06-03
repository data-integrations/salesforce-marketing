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

package io.cdap.plugin.sfmc;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes to Salesforce Marketing Cloud Data Extensions.
 */
@Name("SalesforceDataExtension")
@Description("Inserts records into a Salesforce Marketing Cloud Data Extension.")
@Plugin(type = BatchSink.PLUGIN_TYPE)
public class MarketingCloudDataExtensionSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private final MarketingCloudConf conf;
  private Map<String, String> columnMapping;
  private Schema mappedSchema;

  public MarketingCloudDataExtensionSink(MarketingCloudConf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Schema mappedSchema = inputSchema == null ? null : getMappedSchema(conf.getColumnMapping(inputSchema), inputSchema);
    conf.validate(mappedSchema);
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) {
    Schema inputSchema = batchSinkContext.getInputSchema();
    Map<String, String> columnMapping = conf.getColumnMapping(inputSchema);
    Schema mappedSchema = inputSchema == null ? null : getMappedSchema(columnMapping, inputSchema);
    conf.validate(mappedSchema);

    LineageRecorder lineageRecorder = new LineageRecorder(batchSinkContext, conf.getReferenceName());
    lineageRecorder.createExternalDataset(mappedSchema);
    if (inputSchema != null && mappedSchema.getFields() != null) {
      List<FieldOperation> fieldOperations = new ArrayList<>();
      for (Map.Entry<String, String> mapping : columnMapping.entrySet()) {
        fieldOperations.add(new FieldTransformOperation("Rename", "Renamed a field",
                                                   Collections.singletonList(mapping.getKey()), mapping.getValue()));
      }

      EndPoint endPoint = EndPoint.of(batchSinkContext.getNamespace(), conf.getReferenceName());
      List<String> outputFields = mappedSchema.getFields().stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toList());
      fieldOperations.add(
        new FieldWriteOperation("Write", String.format("Wrote to Salesforce Marketing Cloud Data Extension %s",
                                                       conf.getDataExtension()),
                                endPoint, outputFields));
      batchSinkContext.record(fieldOperations);
    }

    batchSinkContext.addOutput(Output.of(conf.getReferenceName(), new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return DataExtensionOutputFormat.class.getName();
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
        Map<String, String> outputConfig = new HashMap<>();
        outputConfig.put(DataExtensionOutputFormat.CLIENT_ID, conf.getClientId());
        outputConfig.put(DataExtensionOutputFormat.CLIENT_SECRET, conf.getClientSecret());
        outputConfig.put(DataExtensionOutputFormat.AUTH_ENDPOINT, conf.getAuthEndpoint());
        outputConfig.put(DataExtensionOutputFormat.SOAP_ENDPOINT, conf.getSoapEndpoint());
        outputConfig.put(DataExtensionOutputFormat.MAX_BATCH_SIZE, String.valueOf(conf.getMaxBatchSize()));
        outputConfig.put(DataExtensionOutputFormat.FAIL_ON_ERROR, String.valueOf(conf.shouldFailOnError()));
        outputConfig.put(DataExtensionOutputFormat.OPERATION, conf.getOperation().name());
        outputConfig.put(DataExtensionOutputFormat.DATA_EXTENSION_KEY, conf.getDataExtension());
        return outputConfig;
      }
    }));
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    // Ideally this wouldn't be needed, but the UI doesn't allow spaces in the field names
    Schema inputSchema = context.getInputSchema();
    columnMapping = conf.getColumnMapping(inputSchema);
    mappedSchema = getMappedSchema(columnMapping, inputSchema);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    StructuredRecord record = input;
    if (!columnMapping.isEmpty()) {
      StructuredRecord.Builder mapped = StructuredRecord.builder(mappedSchema);
      for (Schema.Field inputField : input.getSchema().getFields()) {
        String fieldName = inputField.getName();
        String mappedName = columnMapping.getOrDefault(fieldName, fieldName);
        mapped.set(mappedName, input.get(fieldName));
      }
      record = mapped.build();
    }

    emitter.emit(new KeyValue<>(NullWritable.get(), record));
  }

  private Schema getMappedSchema(Map<String, String> columnMapping, Schema originalSchema) {
    if (columnMapping.isEmpty()) {
      return originalSchema;
    }
    List<Schema.Field> mappedFields = new ArrayList<>();
    for (Schema.Field inputField : originalSchema.getFields()) {
      String fieldName = inputField.getName();
      String mappedName = columnMapping.getOrDefault(fieldName, fieldName);
      mappedFields.add(Schema.Field.of(mappedName, inputField.getSchema()));
    }
    return Schema.recordOf(originalSchema.getRecordName() + ".mapped", mappedFields);
  }
}
