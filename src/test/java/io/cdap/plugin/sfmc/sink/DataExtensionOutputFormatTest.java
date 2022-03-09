/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class DataExtensionOutputFormatTest {


  protected static final String CLIENT_ID = "clientId";
  protected static final String CLIENT_SECRET = "clientSecret";
  protected static final String AUTH_ENDPOINT = "authEndPoint";
  protected static final String SOAP_ENDPOINT = "soapEndPoint";
  protected static final String OPERATION = "INSERT";
  protected static final String DATA_EXTENSION_KEY = "DE";
  protected static final String MAX_BATCH_SIZE = "500";
  protected static final String FAIL_ON_ERROR = "true";
  protected static final String TRUNCATE = "true";


  MarketingCloudConf config = new MarketingCloudConf("44", CLIENT_ID,
    CLIENT_SECRET, "dataExtension", AUTH_ENDPOINT, SOAP_ENDPOINT, 500,
    "INSERT", "<key=value", false, false, false);

  @Test
  public void testGetRecordWriter() throws IOException {
    JobConf config = new JobConf();
    TaskAttemptID taskId = new TaskAttemptID();
    TaskAttemptContext context = new TaskAttemptContextImpl(config, taskId);
    Configuration conf = context.getConfiguration();
    context.getConfiguration().set(CLIENT_ID, "clientId");
    context.getConfiguration().set(CLIENT_SECRET, "clientSecret");
    context.getConfiguration().set(AUTH_ENDPOINT, "authEndPoint");
    context.getConfiguration().set(SOAP_ENDPOINT, "soapEndPoint");
    context.getConfiguration().set(OPERATION, "INSERT");
    context.getConfiguration().set(DATA_EXTENSION_KEY, "DE");
    context.getConfiguration().set(MAX_BATCH_SIZE, "500");
    context.getConfiguration().set(FAIL_ON_ERROR, "true");
    context.getConfiguration().set(TRUNCATE, "true");
    String clientId = getOrError(conf, CLIENT_ID);
    String clientSecret = getOrError(conf, CLIENT_SECRET);
    String authEndpoint = getOrError(conf, AUTH_ENDPOINT);
    String soapEndpoint = getOrError(conf, SOAP_ENDPOINT);
    String dataExtensionKey = getOrError(conf, DATA_EXTENSION_KEY);
    Operation operation = Operation.valueOf(getOrError(conf, OPERATION));
    int maxBatchSize = Integer.parseInt(getOrError(conf, MAX_BATCH_SIZE));
    boolean failOnError = Boolean.parseBoolean(getOrError(conf, FAIL_ON_ERROR));
    boolean shouldTruncate = Boolean.parseBoolean(getOrError(conf, TRUNCATE));
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    MockFailureCollector collector = new MockFailureCollector();

    try {
      DataExtensionClient client = Mockito.mock(DataExtensionClient.class);
      RecordDataExtensionRowConverter converter = new RecordDataExtensionRowConverter(client.getDataExtensionInfo(),
        shouldTruncate);
      new DataExtensionRecordWriter(client, converter, operation, maxBatchSize, failOnError);
    } catch (ETSdkException e) {
      throw new IOException("Unable to create Salesforce Marketing Cloud client.", e);
    }

  }


  private String getOrError(Configuration conf, String key) {
    String val = conf.get(key);
    if (val == null) {
      throw new IllegalStateException("Missing required value for " + key);
    }
    return val;
  }


}


