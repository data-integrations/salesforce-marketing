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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;

import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;


import io.cdap.plugin.sfmc.source.util.SalesforceObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObjectMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static io.cdap.plugin.sfmc.source.util.SalesforceConstants.PLUGIN_NAME;


/**
 * A {@link BatchSource} that reads data from multiple tables in Service Now.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(PLUGIN_NAME)
@Description("Reads from multiple tables in Service Now. " +
  "Outputs one record for each row in each table, with the table name as a record field. " +
  "Also sets a pipeline argument for each table read, which contains the table schema. ")
public class SalesforceSource extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSource.class);

  private final SalesforceSourceConfig conf;

  public SalesforceSource(SalesforceSourceConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    LOG.debug("Validate config during `configurePipeline` stage: {}", conf);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    conf.validate(collector);
    // Since we have validated all the properties, throw an exception if there are any errors in the collector.
    // This is to avoid adding same validation errors again in getSchema method call
    collector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    conf.validate(collector);
    collector.getOrThrowException();

    SourceObjectMode mode = conf.getQueryMode(collector);

    Configuration hConf = new Configuration();
    Collection<SalesforceObjectInfo> tables = SalesforceInputFormat.setInput(hConf, mode, conf);
   /* SettableArguments arguments = context.getArguments();
    for (SalesforceObjectInfo tableInfo : tables) {
      arguments.set(TABLE_PREFIX + tableInfo.getTableName(), tableInfo.getSchema().toString());
    }

    context.setInput(Input.of(conf.getReferenceName(),
      new SourceInputFormatProvider(ServiceNowInputFormat.class, hConf)));*/
  }

  @Override
  public void transform(KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }
}
