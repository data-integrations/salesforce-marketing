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

import io.cdap.plugin.sfmc.source.util.Util;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UtilTest {


  private static final Logger log = LoggerFactory.getLogger(MarketingCloudSource.class);

  @Test
  public void testIsNullOrEmpty() {
    try {

      boolean expectedValue = false;
      String string = "Name";
      Util util = new Util();
      boolean actualValue = Util.isNullOrEmpty(string);
      Assert.assertEquals(expectedValue, actualValue);

    } catch (Exception exception) {
      log.error("Exception in execution." + exception, exception);
      exception.printStackTrace();
      Assert.assertFalse(false);
    }
  }


  @Test
  public void testSplitToList() {
    try {

      List<String> expectedValue = new ArrayList<>();
      char delimiter = 'a';
      expectedValue.add("N");
      expectedValue.add("me");
      String value = "Name";
      Util util = new Util();
      List<String> actualValue = Util.splitToList(value, delimiter);
      Assert.assertEquals(expectedValue, actualValue);

    } catch (Exception exception) {
      log.error("Exception in execution of execution" + exception, exception);
      exception.printStackTrace();
      Assert.assertFalse(false);
    }
  }
}


