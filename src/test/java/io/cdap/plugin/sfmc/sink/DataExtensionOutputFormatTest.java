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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DataExtensionClient.class)
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

  MarketingCloudConf config = new MarketingCloudConf("44", CLIENT_ID,
    CLIENT_SECRET, "dataExtension", AUTH_ENDPOINT, SOAP_ENDPOINT, 500,
    "INSERT", "<key=value", false, false, false);


  @Test
  public void testCheckOutSpecs() {
    JobContext context1 = Mockito.mock(JobContext.class);
    DataExtensionOutputFormat outputFormat = Mockito.mock(DataExtensionOutputFormat.class);
    Mockito.verify(outputFormat, Mockito.times(0)).checkOutputSpecs(context1);

  }

  @Test
  public void testNeedsTaskCommit() throws IOException {
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    DataExtensionOutputFormat outputFormat = new DataExtensionOutputFormat();
    Assert.assertFalse(outputFormat.getOutputCommitter(taskAttemptContext).needsTaskCommit(taskAttemptContext));
  }


}





