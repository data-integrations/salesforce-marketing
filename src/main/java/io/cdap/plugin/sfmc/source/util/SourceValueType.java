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

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates type of value to be returned from ServiceNow Table API
 */
public enum SourceValueType {

  /**
   * Actual values will be returned by ServiceNow Table API
   */
  SHOW_ACTUAL_VALUE("Actual", "false"),

  /**
   * Display values will be returned by ServiceNow Table API
   */
  SHOW_DISPLAY_VALUE("Display", "true");

  private final String valueType;
  private final String value;

  SourceValueType(String valueType, String value) {
    this.valueType = valueType;
    this.value = value;
  }

  /**
   * Converts value type string value into {@link SourceValueType} enum.
   *
   * @param stringValue value type string value
   * @return source value type in optional container
   */
  public static Optional<SourceValueType> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.valueType.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedValueTypes() {
    return Arrays.stream(SourceValueType.values()).map(SourceValueType::getValueType)
      .collect(Collectors.joining(", "));
  }

  public String getValueType() {
    return valueType;
  }

  public String getValue() {
    return value;
  }
}
