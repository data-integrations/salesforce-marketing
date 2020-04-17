/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.exacttarget.fuelsdk.ETApiObject;
import com.exacttarget.fuelsdk.ETCampaign;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETEmail;
import com.exacttarget.fuelsdk.ETList;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates the object for which data to be fetched from Salesforce.
 */
public enum SourceObject {

  /**
   * This indicates data to be fetched from Data Extension.
   */
  DATA_EXTENSION("Data Extension", "dataextension", ETDataExtensionRow.class),

  /**
   * This indicates data to be fetched from Campaign.
   */
  CAMPAIGN("Campaign", "campaign", ETCampaign.class),

  /**
   * This indicates data to be fetched from Email.
   */
  EMAIL("Email", "email", ETEmail.class),

  /**
   * This indicates data to be fetched from Mailing List.
   */
  MAILING_LIST("Mailing List", "mailinglist", ETList.class);

  private final String value;
  private final String tableName;
  private final Class<? extends ETApiObject> classRef;

  SourceObject(String value, String tableName, Class<? extends ETApiObject> classRef) {
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

  public Class<? extends ETApiObject> getClassRef() {
    return classRef;
  }
}
