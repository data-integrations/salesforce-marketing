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

package io.cdap.plugin.utils.enums;


/**
 * Sobjects - enum.
 */

public enum Sobjects {

  BOUNCE_EVENT("Bounce Event"),
  DATA_EXTENSION("Data Extension"),
  EMAIL("Email"),
  MAILING_LIST("Mailing List"),
  NOTSENT_EVENT("Notsent Event"),
  OPEN_EVENT("Open Event"),
  SENT_EVENT("Sent Event"),
  UNSUB_EVENT("Unsub Event");

  public final String value;

  Sobjects(String value) {
    this.value = value;
  }
}
