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

package io.cdap.plugin.sfmc.sink;

import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configuration for Marketing Cloud plugins.
 */
public class MarketingCloudConf extends PluginConfig {
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String DATA_EXTENSION = "dataExtension";
  public static final String AUTH_ENDPOINT = "authEndpoint";
  public static final String SOAP_ENDPOINT = "soapEndpoint";
  public static final String ACCOUNT_ID = "accountId";
  public static final String BATCH_SIZE = "maxBatchSize";
  public static final String FAIL_ON_ERROR = "failOnError";
  public static final String OPERATION = "operation";
  public static final String COLUMN_MAPPING = "columnMapping";
  public static final String REPLACE_WITH_SPACES = "replaceWithSpaces";
  public static final String TRUNCATE_TEXT = "truncateText";

  @Description("This will be used to uniquely identify this sink for lineage, annotating metadata, etc.")
  private String referenceName;

  @Macro
  @Name(CLIENT_ID)
  @Description("Client ID to use when authenticating with the Marketing Cloud.")
  private String clientId;

  @Macro
  @Name(CLIENT_SECRET)
  @Description("Client secret to use when authenticating with the Marketing Cloud.")
  private String clientSecret;

  @Macro
  @Name(DATA_EXTENSION)
  @Description("Key of the Marketing Cloud Data Extension to insert into.")
  private String dataExtension;

  @Macro
  @Name(AUTH_ENDPOINT)
  @Description("Endpoint to use when authenticating with the Marketing Cloud.")
  private String authEndpoint;

  @Macro
  @Name(SOAP_ENDPOINT)
  @Description("Endpoint to use when communicating with the Marketing Cloud SOAP API.")
  private String soapEndpoint;

  @Macro
  @Name(ACCOUNT_ID)
  @Description("Account ID to use when communicating with the Marketing Cloud SOAP API.")
  private String accountId;

  @Macro
  @Nullable
  @Name(BATCH_SIZE)
  @Description("Maximum number of records to write in a single call to the Marketing Cloud API.")
  private Integer maxBatchSize;

  @Macro
  @Nullable
  @Name(FAIL_ON_ERROR)
  @Description("Whether to fail the pipeline if a record fails to write.")
  private Boolean failOnError;

  @Macro
  @Nullable
  @Name(OPERATION)
  @Description("Type of write operation to perform. This can be set to insert or update.")
  private String operation;

  @Macro
  @Nullable
  @Name(COLUMN_MAPPING)
  @Description("Mapping from input field name to the corresponding data extension column name.")
  private String columnMapping;

  @Macro
  @Nullable
  @Name(REPLACE_WITH_SPACES)
  @Description("Whether to replace underscores in the input field names with spaces.")
  private Boolean replaceWithSpaces;

  @Macro
  @Nullable
  @Name(TRUNCATE_TEXT)
  @Description("Whether to truncate text that is longer than the max length specified in the data extension column.")
  private Boolean truncateText;

  String getReferenceName() {
    return referenceName;
  }

  String getClientId() {
    return clientId;
  }

  String getClientSecret() {
    return clientSecret;
  }

  String getDataExtension() {
    return dataExtension;
  }

  String getAuthEndpoint() {
    return authEndpoint;
  }

  String getSoapEndpoint() {
    return soapEndpoint;
  }

  String getAccountId() {
    return accountId;
  }

  int getMaxBatchSize() {
    return maxBatchSize == null ? 500 : maxBatchSize;
  }

  boolean shouldFailOnError() {
    return failOnError == null ? false : failOnError;
  }

  boolean shouldReplaceWithSpaces() {
    return replaceWithSpaces == null ? false : replaceWithSpaces;
  }

  boolean shouldTruncateText() {
    return truncateText == null ? false : truncateText;
  }

  Operation getOperation() {
    return operation == null ? Operation.INSERT : Operation.valueOf(operation.toUpperCase());
  }

  Map<String, String> getColumnMapping(@Nullable Schema originalSchema, FailureCollector collector) {
    Set<String> fieldNames = originalSchema == null ? Collections.emptySet() :
      originalSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toSet());
    Map<String, String> mapping = new HashMap<>();

    if (columnMapping != null) {
      for (String kv : columnMapping.split(";")) {
        String[] parts = kv.split("=");
        if (parts.length != 2) {
          collector.addFailure(String.format("Invalid column mapping: %s", kv),
                               "Make sure column mapping is in the format of <key=value>")
            .withConfigProperty(COLUMN_MAPPING);
          throw collector.getOrThrowException();
        }
        if (fieldNames.contains(parts[0])) {
          mapping.put(parts[0], parts[1]);
        }
      }
    }

    if (shouldReplaceWithSpaces()) {
      for (String fieldName : fieldNames) {
        if (mapping.containsKey(fieldName)) {
          continue;
        }
        if (fieldName.contains("_")) {
          mapping.put(fieldName, fieldName.replaceAll("_", " "));
        }
      }
    }

    return mapping;
  }

  public void validate(@Nullable Schema inputSchema, FailureCollector collector) {
    if (inputSchema == null) {
      return;
    }
    if (!containsMacro(CLIENT_ID) && !containsMacro(CLIENT_SECRET) && !containsMacro(DATA_EXTENSION) &&
      !containsMacro(AUTH_ENDPOINT) && !containsMacro(SOAP_ENDPOINT) && !containsMacro(ACCOUNT_ID)) {
      try {
        DataExtensionClient client = DataExtensionClient.create(dataExtension, clientId, clientSecret,
                                                                authEndpoint, soapEndpoint, accountId);
        client.validateSchemaCompatibility(inputSchema, collector);
      } catch (ETSdkException e) {
        collector.addFailure("Error while validating Marketing Cloud client: " + e.getMessage(), null)
          .withStacktrace(e.getStackTrace());
      }
    }
    if (!containsMacro(BATCH_SIZE)) {
      int batchSize = getMaxBatchSize();
      if (batchSize < 0) {
        collector.addFailure(String.format("Invalid batch size '%d'.", batchSize),
                             "The batch size must be at least 1.").withConfigProperty(BATCH_SIZE);
      }
    }
  }
}
