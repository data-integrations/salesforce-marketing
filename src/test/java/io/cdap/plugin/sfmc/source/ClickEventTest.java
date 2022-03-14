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
import com.custom.fuelsdk.internal.ClickEvent;

import org.junit.Assert;
import org.junit.Test;

public class ClickEventTest {

  @Test
  public void testUrlId() {
    ClickEvent clickEvent = new ClickEvent();
    clickEvent.setUrlid(121);
    Assert.assertFalse(clickEvent.getUrlid().toString().isEmpty());
  }

  @Test
  public void testUrl() {
    ClickEvent clickEvent = new ClickEvent();
    clickEvent.setUrl("http://www.w3.org/2001/XMLSchema");
    Assert.assertEquals("http://www.w3.org/2001/XMLSchema", clickEvent.getUrl());
  }

  @Test
  public void testUrlIdLong() {
    ClickEvent clickEvent = new ClickEvent();
    clickEvent.setUrlIdLong(987654L);
    Assert.assertFalse(clickEvent.getUrlIdLong().toString().isEmpty());
  }

  @Test
  public void testIdNull() {
    PaginationETSoapObject paginationETSoapObject = new PaginationETSoapObject();
    paginationETSoapObject.setId(null);
    Assert.assertEquals(null, paginationETSoapObject.getId());


  }

  @Test
  public void testToString() {
    ClickEvent clickEvent = new ClickEvent();
    clickEvent.setUrl("http://www.w3.org/2001/XMLSchema");
    Assert.assertFalse(clickEvent.toString().isEmpty());
  }
}

