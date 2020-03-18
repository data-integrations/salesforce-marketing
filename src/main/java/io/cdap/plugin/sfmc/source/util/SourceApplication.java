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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates application for which data to be fetched from multiple tables in ServiceNow.
 */
public enum SourceApplication {

  /**
   * Data from tables related to Contract Management will be fetched
   */
  CONTRACT_MANAGEMENT("Contract Management", Arrays.asList("clm_m2m_contract_asset", "clm_m2m_rate_card_asset",
    "clm_condition_checker", "clm_condition_check", "ast_contract", "clm_contract_history",
    "clm_terms_and_conditions", "clm_m2m_contract_and_terms", "clm_m2m_contract_user")),

  /**
   * Data from tables related to Product Catalog will be fetched
   */
  PRODUCT_CATALOG("Product Catalog", Arrays.asList("new_call", "pc_hardware_cat_item", "pc_product_cat_item",
    "pc_software_cat_item", "pc_vendor_cat_item")),

  /**
   * Data from tables related to Procurement will be fetched
   */
  PROCUREMENT("Procurement", Arrays.asList("proc_po", "proc_po_item", "proc_rec_slip", "proc_rec_slip_item"));

  private final String value;
  private final List<String> tableNames;

  SourceApplication(String value, List<String> tableNames) {
    this.value = value;
    this.tableNames = tableNames;
  }

  /**
   * Converts application string value into {@link SourceApplication} enum.
   *
   * @param stringValue application string value
   * @return source application in optional container
   */
  public static Optional<SourceApplication> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedApplications() {
    return Arrays.stream(SourceApplication.values()).map(SourceApplication::getValue)
      .collect(Collectors.joining(", "));
  }

  public String getValue() {
    return value;
  }

  public List<String> getTableNames() {
    return tableNames;
  }
}
