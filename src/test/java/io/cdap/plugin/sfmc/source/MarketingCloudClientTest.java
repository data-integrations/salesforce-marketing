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
package io.cdap.plugin.sfmc.source;

import com.custom.fuelsdk.PaginationETSoapObject;
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETExpression;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.plugin.sfmc.source.util.SourceObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ETExpression.class, ETClient.class, PaginationETSoapObject.class})
public class MarketingCloudClientTest {

  @Test
  public void testFetchObjectRecordsWNull() throws Exception {
    SourceObject object = SourceObject.TRACKING_UNSUB_EVENT;
    PowerMockito.spy(MarketingCloudClient.class);
    PowerMockito.mockStatic(PaginationETSoapObject.class);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    MarketingCloudClient client = new MarketingCloudClient(etClient);
    Assert.assertNull(client.fetchObjectRecords(object, "IsUnique = 1", null));
  }

  @Test
  public void testFetchDataExtensionRecordsWNull() throws Exception {
    String dataExtensionKey = "DE";
    PowerMockito.spy(MarketingCloudClient.class);
    PowerMockito.mockStatic(PaginationETSoapObject.class);
    ETClient etClient = PowerMockito.mock(ETClient.class);
    PowerMockito.whenNew(ETClient.class).withArguments(Mockito.anyString()).thenReturn(etClient);
    MarketingCloudClient client = new MarketingCloudClient(etClient);
    Assert.assertNull(client.fetchDataExtensionRecords(dataExtensionKey, null, null));
    client.fetchDataExtensionRecords(dataExtensionKey, null, null);
    Assert.assertNull(client.fetchDataExtensionRecords(dataExtensionKey, null, null));
  }

  @Test
  public void testValidateFilter() {
    String filter = "filter";
    ETClient etClient = Mockito.mock(ETClient.class);
    MarketingCloudClient marketingCloudClient = new MarketingCloudClient(etClient);
    try {
      marketingCloudClient.validateFilter(filter);
    } catch (ETSdkException e) {
    }
  }
}
