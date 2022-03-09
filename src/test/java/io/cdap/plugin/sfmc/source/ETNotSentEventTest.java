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

import com.custom.fuelsdk.ETNotSentEvent;
import com.exacttarget.fuelsdk.internal.EventType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class ETNotSentEventTest {

  @Test
  public void testSendId() {
    ETNotSentEvent event = new ETNotSentEvent();
    event.setSendID(123);
    Assert.assertFalse(event.getSendID().toString().isEmpty());
  }

  @Test
  public void testSubscriberKey() {
    ETNotSentEvent event = new ETNotSentEvent();
    event.setSubscriberKey("Key");
    Assert.assertEquals("Key", event.getSubscriberKey());
  }

  @Test
  public void testBatchId() {
    ETNotSentEvent event = new ETNotSentEvent();
    event.setBatchID(1234);
    Assert.assertFalse(event.getBatchID().toString().isEmpty());
  }

  @Test
  public void testEventDate() {
    ETNotSentEvent event = new ETNotSentEvent();
    Date d = new Date(21, 9, 2022);
    event.setEventDate(d);
    Assert.assertFalse(event.getEventDate().toString().isEmpty());
  }

  @Test
  public void testEventType() {
    ETNotSentEvent event = new ETNotSentEvent();
    event.setEventType(EventType.NOT_SENT);
    Assert.assertEquals(EventType.NOT_SENT, event.getEventType());
  }

  @Test
  public void testTriggeredSendDefinitionObjectID() {
    ETNotSentEvent event = new ETNotSentEvent();
    event.setTriggeredSendDefinitionObjectID("1234");
    Assert.assertFalse(event.getTriggeredSendDefinitionObjectID().isEmpty());
  }
}
