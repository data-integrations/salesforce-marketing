
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

import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETConfiguration;
import com.exacttarget.fuelsdk.ETDataExtension;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.plugin.sfmc.source.SalesforceSourceConfig;
import io.cdap.plugin.sfmc.source.apiclient.SalesforceTableAPIClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * TokenGenerator class.
 */
public class TokenGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TokenGenerator.class);

    public ETClient getAPIDetails(SalesforceSourceConfig conf) throws ETSdkException {

        ETConfiguration configuration = new ETConfiguration();
      /*  configuration.setClientId("isftmkzk5sr7olx6em78otnw");
        configuration.setClientSecret("OHT528TeEsDd5dHtrQOUjWtl");
        configuration.setSoapEndpoint("https://mc-67-bn30-84yc47k-pw5rw0vp1.soap.marketingcloudapis.com/Service.asmx");
        configuration.setEndpoint("https://mc-67-bn30-84yc47k-pw5rw0vp1.rest.marketingcloudapis.com/");
        configuration.setAuthEndpoint("https://mc-67-bn30-84yc47k-pw5rw0vp1.auth.marketingcloudapis.com/");*/
       /* configuration.set("clientId", conf.getClientId());
        configuration.set("clientSecret", conf.getClientSecret());
        configuration.set("soapEndpoint", conf.getSoapEndpoint());
        configuration.set("endpoint", conf.getRestEndpoint());
        configuration.set("authEndpoint", conf.getAuthEndpoint());
        configuration.set("useOAuth2Authentication", "true");
        ETClient client = new ETClient(configuration);*/
        configuration.set("clientId", "isftmkzk5sr7olx6em78otnw");
        configuration.set("clientSecret", "OHT528TeEsDd5dHtrQOUjWtl");
        configuration.set("soapEndpoint",
                "https://mc-67-bn30-84yc47k-pw5rw0vp1.soap.marketingcloudapis.com/Service.asmx");
        configuration.set("endpoint", "https://mc-67-bn30-84yc47k-pw5rw0vp1.rest.marketingcloudapis.com/");
        configuration.set("authEndpoint", "https://mc-67-bn30-84yc47k-pw5rw0vp1.auth.marketingcloudapis.com/");
        configuration.set("useOAuth2Authentication", "true");
        ETClient client = new ETClient(configuration);

       /* ETFilter etf = new ETFilter();
        etf.addProperty("id");
        etf.addProperty("key");
        etf.addProperty("name");
        etf.addProperty("description");
        etf.addProperty("folderId");
        etf.addProperty("isSendable");
        etf.addProperty("isTestable");*


        /* Get list of all Data Extensions */

      /*  ETResponse<ETDataExtension> dtresponse = client.retrieve(ETDataExtension.class, etf);
        for (ETDataExtension dt : dtresponse.getObjects()) {
            System.out.println("ETDataExtension--->" + dt);
            LOG.info("ETDataExtension--->" + dt);
            ETResponse<ETDataExtensionRow> resp = dt.select();
            List<ETDataExtensionRow> rows = resp.getObjects();
            if (rows.size() > 1) {
                ETDataExtensionRow row1 = rows.get(0);
                LOG.info("row records--->" + row1);
            }
        }*/
        return client;

    }
}
