/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.sfmc.source.apiclient;



import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import io.cdap.plugin.sfmc.DataExtensionClient;
import io.cdap.plugin.sfmc.source.SalesforceSourceConfig;
import io.cdap.plugin.sfmc.source.util.SalesforceColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

import java.util.List;
import java.util.Map;

/**
 * Implementation class for Salesforce Table API
 */
public class SalesforceTableAPIClientImpl {
    private static final Logger LOG = LoggerFactory.getLogger(SalesforceTableAPIClientImpl.class);


    private SalesforceSourceConfig conf;

    public SalesforceTableAPIClientImpl(SalesforceSourceConfig conf) {
        this.conf = conf;


        try {
            DataExtensionClient client = client = DataExtensionClient.create("DATAEXTENSIONKEY",
                    conf.getClientId(), conf.getClientSecret(),
                    conf.getAuthEndpoint(), conf.getSoapEndpoint());

            LOG.info("getDataExtensionInfo  {}, getDataExtensionKey  {}",
                    client.getDataExtensionInfo(), client.getDataExtensionKey());
        } catch (Exception ets) {
            ets.printStackTrace();
        }
    }


    private List<Map<String, Object>> parseResponseToResultListOfMap(String responseBody) {
        Gson gson = new Gson();
        JsonObject jo = gson.fromJson(responseBody, JsonObject.class);
        JsonArray ja = jo.getAsJsonArray("result");

        Type type = new TypeToken<List<Map<String, Object>>>() {
        }.getType();

        return gson.fromJson(ja, type);
    }

    private List<SalesforceColumn> parseResponseToResultListOfPojo(String responseBody) {
        Gson gson = new Gson();
        JsonObject jo = gson.fromJson(responseBody, JsonObject.class);
        JsonArray ja = jo.getAsJsonArray("result");

        Type type = new TypeToken<List<SalesforceColumn>>() {
        }.getType();

        return gson.fromJson(ja, type);
    }

    private String getErrorMessage(String responseBody) {
        try {
            Gson gson = new Gson();
            JsonObject jo = gson.fromJson(responseBody, JsonObject.class);
            return jo.getAsJsonObject("error").get("message").getAsString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }
}
