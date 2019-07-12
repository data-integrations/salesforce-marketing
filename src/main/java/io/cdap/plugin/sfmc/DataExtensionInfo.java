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

package io.cdap.plugin.sfmc;

import com.exacttarget.fuelsdk.ETDataExtensionColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Information about a data extension.
 */
public class DataExtensionInfo {
  private final String externalKey;
  private final List<ETDataExtensionColumn> columnList;
  private final Map<String, ETDataExtensionColumn> columns;

  public DataExtensionInfo(String externalKey, List<ETDataExtensionColumn> columnList) {
    this.externalKey = externalKey;
    this.columnList = Collections.unmodifiableList(new ArrayList<>(columnList));
    this.columns = columnList.stream()
      .collect(Collectors.toMap(c -> c.getName().toLowerCase(), c -> c));
  }

  public String getExternalKey() {
    return externalKey;
  }

  public List<ETDataExtensionColumn> getColumnList() {
    return columnList;
  }

  public boolean hasColumn(String name) {
    return columns.containsKey(name.toLowerCase());
  }

  @Nullable
  public ETDataExtensionColumn getColumn(String name) {
    return columns.get(name.toLowerCase());
  }
}
