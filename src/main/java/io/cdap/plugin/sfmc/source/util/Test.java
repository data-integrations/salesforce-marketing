
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

import java.util.List;

/**
 * TokenGenerator class.
 */
public class Test {
    public static void main(String[] args)
            throws ETSdkException {
        SalesforceSourceConfig conf = null;
        TokenGenerator tokenGenerator = new TokenGenerator();
        long m = System.currentTimeMillis() / 1000;

        tokenGenerator.getAPIDetails(conf);
        long n = System.currentTimeMillis() / 1000;

        System.out.println("Total second required" + (n - m));
        //ETClient client = new ETClient("/Users/akhilesh.trivedi/gitcdap/FuelSDK-Java-Example/src/main/java/com
        // /exacttarget/fuelsdk/test/fuelsdk.properties");
        /*
        ETResponse<ETFolder> response = client.retrieve(ETFolder.class);
        for (ETFolder folder : response.getObjects()) {
            System.out.println(folder);
        }
        */
        //      ETConfiguration configuration = new ETConfiguration();
      /*  configuration.setClientId("isftmkzk5sr7olx6em78otnw");
        configuration.setClientSecret("OHT528TeEsDd5dHtrQOUjWtl");
        configuration.setSoapEndpoint("https://mc-67-bn30-84yc47k-pw5rw0vp1.soap.marketingcloudapis.com/Service.asmx");
        configuration.setEndpoint("https://mc-67-bn30-84yc47k-pw5rw0vp1.rest.marketingcloudapis.com/");
        configuration.setAuthEndpoint("https://mc-67-bn30-84yc47k-pw5rw0vp1.auth.marketingcloudapis.com/");*/
        /*configuration.set("clientId" , "isftmkzk5sr7olx6em78otnw");
        configuration.set("clientSecret" , "OHT528TeEsDd5dHtrQOUjWtl");
        configuration.set("soapEndpoint" ,
                "https://mc-67-bn30-84yc47k-pw5rw0vp1.soap.marketingcloudapis.com/Service.asmx");
        configuration.set("endpoint" , "https://mc-67-bn30-84yc47k-pw5rw0vp1.rest.marketingcloudapis.com/");
        configuration.set("authEndpoint" , "https://mc-67-bn30-84yc47k-pw5rw0vp1.auth.marketingcloudapis.com/");
        configuration.set("useOAuth2Authentication" , "true");
        ETClient client = new ETClient(configuration);

        ETFilter etf = new ETFilter();
        etf.addProperty("id");
        etf.addProperty("key");
        etf.addProperty("name");
        etf.addProperty("description");
        etf.addProperty("folderId");
        etf.addProperty("isSendable");
        etf.addProperty("isTestable");
*/

        /* Get list of all Data Extensions */

       /* ETResponse<ETDataExtension> dtresponse = client.retrieve(ETDataExtension.class,etf);
        for (ETDataExtension dt : dtresponse.getObjects()) {
            System.out.println("I am here--->"+dt);
            ETResponse<ETDataExtensionRow> resp = dt.select();
            List<ETDataExtensionRow> rows = resp.getObjects();
            if ( rows.size() > 1 ) {
                ETDataExtensionRow row1 = rows.get(0);
                System.out.println("row--->"+row1);
            }
        }*/

        /* Get Rows in each Data Extension

        ETResponse<ETCampaign> campaignETResponse = client.retrieve(ETCampaign.class,etf);
        System.out.println(campaignETResponse.getResponseMessage());

        */


    }
}
