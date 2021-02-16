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

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates query mode which will be used when fetching MarketingCloud data.
 */
public enum SourceQueryMode {

  /**
   * Mode will be used to fetch data for multiple objects.
   */
  MULTI_OBJECT("Multi Object"),

  /**
   * Mode will be used to fetch data for single object.
   */
  SINGLE_OBJECT("Single Object");

  private final String value;

  SourceQueryMode(String value) {
    this.value = value;
  }

  /**
   * Converts mode string value into {@link SourceQueryMode} enum.
   *
   * @param stringValue mode string value
   * @return source query mode in optional container
   */
  public static Optional<SourceQueryMode> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedModes() {
    return Arrays.stream(SourceQueryMode.values()).map(SourceQueryMode::getValue)
      .collect(Collectors.joining(", "));
  }

  public String getValue() {
    return value;
  }
}
