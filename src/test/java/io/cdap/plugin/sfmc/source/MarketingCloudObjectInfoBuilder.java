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

import io.cdap.plugin.sfmc.source.util.MarketingCloudColumn;
import io.cdap.plugin.sfmc.source.util.MarketingCloudObjectInfo;
import io.cdap.plugin.sfmc.source.util.SourceObject;

import java.util.ArrayList;
import java.util.List;

public class MarketingCloudObjectInfoBuilder {


  SourceObject object = SourceObject.DATA_EXTENSION;
  String dataExtensionKey = "dataExtensionKey";
  List<MarketingCloudColumn> columns = new ArrayList<>();


  public MarketingCloudObjectInfoBuilder setObject(SourceObject object) {
    this.object = object;
    return this;
  }


  public MarketingCloudObjectInfoBuilder setDataExtensionKey(String dataExtensionKey) {
    this.dataExtensionKey = dataExtensionKey;
    return this;

  }

  public MarketingCloudObjectInfoBuilder setColumns(List<MarketingCloudColumn> columns) {
    this.columns = columns;
    return this;
  }

  public MarketingCloudObjectInfo build() {
    return new MarketingCloudObjectInfo(object, dataExtensionKey, columns);
  }

}


