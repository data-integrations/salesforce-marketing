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
package io.cdap.plugin.sfmc.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.sfmc.connector.MarketingConnectorConfig;

import javax.annotation.Nullable;

/**
 * Salesforce Marketing Cloud Base Config
 */
public class SalesforceMarketingCloudBaseConfig extends ReferencePluginConfig {

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private MarketingConnectorConfig connection;

  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  public SalesforceMarketingCloudBaseConfig(String referenceName, String clientId, String clientSecret,
                                            String authEndpoint, String soapEndpoint) {
    super(referenceName);
    this.connection = new MarketingConnectorConfig(clientId, clientSecret, authEndpoint, soapEndpoint);
  }

  @Nullable
  public MarketingConnectorConfig getConnection() {
    return connection;
  }

  public String getReferenceName() {
    return referenceName;
  }
}
