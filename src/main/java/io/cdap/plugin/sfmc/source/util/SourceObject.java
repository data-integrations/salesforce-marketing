/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.sfmc.source.util;

import com.custom.fuelsdk.ETNotSentEvent;
import com.exacttarget.fuelsdk.ETBounceEvent;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETEmail;
import com.exacttarget.fuelsdk.ETList;
import com.exacttarget.fuelsdk.ETOpenEvent;
import com.exacttarget.fuelsdk.ETSentEvent;
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.ETUnsubEvent;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates the object for which data to be fetched from MarketingCloud.
 */
public enum SourceObject {

  /**
   * This indicates data to be fetched from Data Extension.
   */
  DATA_EXTENSION("Data Extension", "dataextension", ETDataExtensionRow.class),

  /**
   * This indicates data to be fetched from Campaign.
   */
//  CAMPAIGN("Campaign", "campaign", ETCampaign.class),

  /**
   * This indicates data to be fetched from Email.
   */
  EMAIL("Email", "email", ETEmail.class),

  /**
   * This indicates data to be fetched from Mailing List.
   */
  MAILING_LIST("Mailing List", "mailinglist", ETList.class),

  /**
   * This indicates data to be fetched from Tracking Bounce Events.
   */
  TRACKING_BOUNCE_EVENT("Bounce Event", "bounce", ETBounceEvent.class),

  /**
   * This indicates data to be fetched from Tracking Open Events.
   */
  TRACKING_OPEN_EVENT("Open Event", "open", ETOpenEvent.class),

  /**
   * This indicates data to be fetched from Tracking Click Events.
   */
  //TRACKING_CLICK_EVENT("Click Event", "click", ETClickEvent.class),

  /**
   * This indicates data to be fetched from Tracking UnSub Events.
   */
  TRACKING_UNSUB_EVENT("Unsub Event", "unsub", ETUnsubEvent.class),

  /**
   * This indicates data to be fetched from Tracking Sent Events.
   */
  TRACKING_SENT_EVENT("Sent Event", "sent", ETSentEvent.class),

  /**
   * This indicates data to be fetched from Tracking Notsent Events.
   */
  TRACKING_NOTSENT_EVENT("Notsent Event", "notsent", ETNotSentEvent.class);

  private final String value;
  private final String tableName;
  private final Class<? extends ETSoapObject> classRef;
  private String filter = "";


  SourceObject(String value, String tableName, Class<? extends ETSoapObject> classRef) {
    this.value = value;
    this.tableName = tableName;
    this.classRef = classRef;
  }

  /**
   * Converts object string value into {@link SourceObject} enum.
   *
   * @param stringValue object string value
   * @return source object in optional container
   */
  public static Optional<SourceObject> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedObjects() {
    return Arrays.stream(SourceObject.values()).map(SourceObject::getValue)
      .collect(Collectors.joining(", "));
  }

  public String getValue() {
    return value;
  }

  public String getTableName() {
    return tableName;
  }

  public Class<? extends ETSoapObject> getClassRef() {
    return classRef;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

}
