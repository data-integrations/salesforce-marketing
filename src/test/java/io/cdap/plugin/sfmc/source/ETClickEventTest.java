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

import com.custom.fuelsdk.ETClickEvent;
import com.exacttarget.fuelsdk.internal.EventType;
import org.junit.Assert;
import org.junit.Test;
import java.util.Date;

public class ETClickEventTest {

  @Test
  public void testURLId() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setUrlid(123);
    Assert.assertFalse(etClickEvent.getUrlid().toString().isEmpty());
  }

  @Test
  public void testURL() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setUrl("www.id.com");
    Assert.assertEquals(etClickEvent.getUrl(), "www.id.com");
  }

  @Test
  public void testBatchId() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setBatchID(121);
    Assert.assertFalse(etClickEvent.getBatchID().toString().isEmpty());
  }

  @Test
  public void testId() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setId("id");
    Assert.assertEquals(etClickEvent.getId(), "id");
  }

  @Test
  public void testSendId() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setSendID(121);
    Assert.assertFalse(etClickEvent.getSendID().toString().isEmpty());
  }

  @Test
  public void testSubscriberKey() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setSubscriberKey("key");
    Assert.assertEquals(etClickEvent.getSubscriberKey(), "key");
  }

  @Test
  public void testEventType() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setEventType(EventType.CLICK);
    Assert.assertEquals(etClickEvent.getEventType(), EventType.CLICK);
  }

  @Test
  public void testEventDate() {
    ETClickEvent etClickEvent = new ETClickEvent();
    Date d = new Date(2022, 01, 01);
    etClickEvent.setEventDate(d);
    Assert.assertFalse(etClickEvent.getEventDate().toString().isEmpty());
  }

  @Test
  public void testTriggeredDefinitionId() {
    ETClickEvent etClickEvent = new ETClickEvent();
    etClickEvent.setTriggeredSendDefinitionObjectID("triggeredId");
    Assert.assertEquals(etClickEvent.getTriggeredSendDefinitionObjectID(), "triggeredId");
  }
}
