
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

import com.exacttarget.fuelsdk.ETConfiguration;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.plugin.sfmc.DataExtensionClient;
import io.cdap.plugin.sfmc.source.SalesforceSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * TableConnection class.
 * A data extension is a table that contains your data
 */
public class TableConnection {
    private static final Logger LOG = LoggerFactory.getLogger(TableConnection.class);

    public DataExtensionClient getTableConnection(SalesforceSourceConfig conf) {
        DataExtensionClient client = null;
        try {
            client = DataExtensionClient.create("2E2DF3B8-F3A0-415C-9EFC-922D32E13E61",
                    conf.getClientId(), conf.getClientSecret(),
                    conf.getAuthEndpoint(), conf.getSoapEndpoint());

        } catch (ETSdkException e) {
            try {
                throw new IOException("Unable to create Salesforce Marketing Cloud client.", e);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        return client;

    }
}
