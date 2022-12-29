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

import com.exacttarget.fuelsdk.ETClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataExtensionClient.class, ETClient.class})
public class DataExtensionOutputFormatTest {
  public static final String CLIENT_ID = "cdap.sfmc.client.id";
  public static final String CLIENT_SECRET = "cdap.sfmc.client.secret";
  public static final String AUTH_ENDPOINT = "cdap.sfmc.auth.endpoint";
  public static final String SOAP_ENDPOINT = "cdap.sfmc.soap.endpoint";
  public static final String DATA_EXTENSION_KEY = "cdap.sfmc.data.extension.key";
  public static final String MAX_BATCH_SIZE = "cdap.sfmc.max.batch.size";
  public static final String FAIL_ON_ERROR = "cdap.sfmc.fail.on.error";
  public static final String OPERATION = "cdap.sfmc.operation";
  public static final String TRUNCATE = "cdap.sfmc.truncate";

  @Test
  public void testNeedsTaskCommit() throws IOException {
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    Assert.assertFalse(outputFormat.getOutputCommitter(taskAttemptContext).needsTaskCommit(taskAttemptContext));
  }

  @Test
  public void testSetUpTask() throws IOException {
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    outputFormat.getOutputCommitter(taskAttemptContext).setupTask(taskAttemptContext);
  }

  @Test
  public void testSetUpJob() throws IOException {
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    JobContext jobContext = Mockito.mock(JobContext.class);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    outputFormat.getOutputCommitter(taskAttemptContext).setupJob(jobContext);
  }

  @Test
  public void testCommitTask() throws IOException {
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    outputFormat.getOutputCommitter(taskAttemptContext).commitTask(taskAttemptContext);
  }

  @Test
  public void testAbortTask() throws IOException {
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    outputFormat.getOutputCommitter(taskAttemptContext).abortTask(taskAttemptContext);
  }

  @Test
  public void testCheckOutputSpecs() {
    JobContext jobContext = Mockito.mock(JobContext.class);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    outputFormat.checkOutputSpecs(jobContext);
  }

  @Test
  public void testGetRecordWriter() throws Exception {
    TaskAttemptContext context = Mockito.mock(TaskAttemptContext.class);
    Configuration conf = new Configuration();
    conf.set(CLIENT_ID, "clientid");
    conf.set(CLIENT_SECRET, "clientSecret");
    conf.set(AUTH_ENDPOINT, "authEndPoint");
    conf.set(SOAP_ENDPOINT, "soapEndPoint");
    conf.set(OPERATION, "INSERT");
    conf.set(DATA_EXTENSION_KEY, "DE");
    conf.set(MAX_BATCH_SIZE, "500");
    conf.set(FAIL_ON_ERROR, "true");
    conf.set(TRUNCATE, "true");
    String dataExtensionKey = "DE";
    Mockito.when(context.getConfiguration()).thenReturn(conf);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    PowerMockito.mockStatic(DataExtensionClient.class);
    DataExtensionClient client = PowerMockito.mock(DataExtensionClient.class);
    PowerMockito.whenNew(DataExtensionClient.class).withArguments(Mockito.any(), Mockito.anyString()).
      thenReturn(client);
    PowerMockito.when(DataExtensionClient.create(dataExtensionKey, "clientid", "clientSecret", "authEndPoint",
      "soapEndPoint")).thenReturn(client);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    Assert.assertNotNull(outputFormat.getRecordWriter(context));
  }

  @Test
  public void testGetRecordWriterException() {
    TaskAttemptContext context = Mockito.mock(TaskAttemptContext.class);
    Configuration conf = new Configuration();
    conf.set(CLIENT_ID, "clientid");
    conf.set(CLIENT_SECRET, "clientSecret");
    conf.set(AUTH_ENDPOINT, "authEndPoint");
    conf.set(SOAP_ENDPOINT, "soapEndPoint");
    conf.set(OPERATION, "INSERT");
    conf.set(DATA_EXTENSION_KEY, "DE");
    conf.set(MAX_BATCH_SIZE, "500");
    conf.set(FAIL_ON_ERROR, "true");
    conf.set(TRUNCATE, "true");
    String dataExtensionKey = "DE";
    Mockito.when(context.getConfiguration()).thenReturn(conf);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    try {
      Assert.assertNull(outputFormat.getRecordWriter(context));
    } catch (IOException e) {
      Assert.assertEquals(51, e.getMessage().length());
    }
  }
}
