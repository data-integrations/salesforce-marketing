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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.sfmc.source.util.MarketingCloudConstants;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceQueryMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link BatchSource} that reads data from multiple objects in Salesforce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MarketingCloudConstants.PLUGIN_NAME)
@Description("Read marketing data from Salesforce Marketing cloud.")
public class MarketingCloudSource extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MarketingCloudSource.class);

  private final MarketingCloudSourceConfig conf;

  public MarketingCloudSource(MarketingCloudSourceConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    conf.validate(collector);
    // Since we have validated all the properties, throw an exception if there are any
    // errors in the collector. This is to avoid adding same validation errors again in
    // getSchema method call
    collector.getOrThrowException();

    //Get Schema
    stageConfigurer.setOutputSchema(getSchema(conf.getQueryMode()));

    if (pipelineConfigurer.getEngine() == Engine.SPARK) {
      pipelineConfigurer.setPipelineProperties(Collections.singletonMap("spark.task.maxFailures", "1"));
    } else if (pipelineConfigurer.getEngine() == Engine.MAPREDUCE) {
      Map<String, String> properties = new HashMap<>();
      properties.put("mapreduce.reduce.maxattempts", "1");
      properties.put("mapreduce.map.maxattempts", "1");
      pipelineConfigurer.setPipelineProperties(properties);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    conf.validate(collector);
    collector.getOrThrowException();

    SourceQueryMode mode = conf.getQueryMode(collector);

    Configuration hConf = new Configuration();
    Collection<MarketingCloudObjectInfo> tables = MarketingCloudInputFormat.setInput(hConf, mode, conf);
    SettableArguments arguments = context.getArguments();
    for (MarketingCloudObjectInfo tableInfo : tables) {
      arguments.set(MarketingCloudConstants.TABLE_PREFIX + tableInfo.getFormattedTableName(),
                    tableInfo.getSchema().toString());
      recordLineage(context, tableInfo);
    }

    context.setInput(Input.of(conf.getReferenceName(),
                              new SourceInputFormatProvider(MarketingCloudInputFormat.class, hConf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }

  public Schema getSchema(SourceQueryMode mode) {
    Schema schema = null;
    if (mode == SourceQueryMode.SINGLE_OBJECT) {
      Configuration hConf = new Configuration();
      Collection<MarketingCloudObjectInfo> tables = MarketingCloudInputFormat.setInput(hConf, mode, conf);
      if (tables != null && !tables.isEmpty()) {
        schema = tables.iterator().next().getSchema();
      }
    }
    return schema;
  }

  private void recordLineage(BatchSourceContext context, MarketingCloudObjectInfo tableInfo) {
    String tableName = tableInfo.getFormattedTableName();
    String outputName = String.format("%s-%s", conf.getReferenceName(), tableName);
    Schema schema = tableInfo.getSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema).getFields();
    if (fields != null && !fields.isEmpty()) {
      lineageRecorder.recordRead("Read",
                                 String.format("Read from '%s' Marketing Cloud object.", tableName),
                                 fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}
