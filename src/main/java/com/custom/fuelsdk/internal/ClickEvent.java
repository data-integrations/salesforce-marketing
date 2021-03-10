/*
 * Copyright © 2020 Cask Data, Inc.
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

package com.custom.fuelsdk.internal;

import com.exacttarget.fuelsdk.internal.TrackingEvent;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.cxf.xjc.runtime.JAXBToStringStyle;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for ClickEvent complex type.
 *
 * <p>The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="ClickEvent"&gt;
 *   &lt;complexContent&gt;
 *     &lt;extension base="{http://exacttarget.com/wsdl/partnerAPI}TrackingEvent"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="URLID" type="{http://www.w3.org/2001/XMLSchema}int" minOccurs="0"/&gt;
 *         &lt;element name="URL" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/&gt;
 *         &lt;element name="URLIDLong" type="{http://www.w3.org/2001/XMLSchema}long" minOccurs="0"/&gt;
 *       &lt;/sequence&gt;
 *     &lt;/extension&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ClickEvent", propOrder = {
  "urlid",
  "url",
  "urlIdLong"
})
public class ClickEvent
  extends TrackingEvent {

  @XmlElement(name = "URLID")
  protected Integer urlid;
  @XmlElement(name = "URL")
  protected String url;
  @XmlElement(name = "URLIDLong")
  protected Long urlIdLong;

  /**
   * Gets the value of the urlid property.
   *
   * @return possible object is
   * {@link Integer }
   */
  public Integer getUrlid() {
    return urlid;
  }

  /**
   * Sets the value of the urlid property.
   *
   * @param value allowed object is
   *              {@link Integer }
   */
  public void setUrlid(Integer value) {
    this.urlid = value;
  }

  /**
   * Gets the value of the url property.
   *
   * @return possible object is
   * {@link String }
   */
  public String getUrl() {
    return url;
  }

  /**
   * Sets the value of the url property.
   *
   * @param value allowed object is
   *              {@link String }
   */
  public void setUrl(String value) {
    this.url = value;
  }

  /**
   * Gets the value of the urlIdLong property.
   *
   * @return possible object is
   * {@link Long }
   */
  public Long getUrlIdLong() {
    return urlIdLong;
  }

  /**
   * Sets the value of the urlIdLong property.
   *
   * @param value allowed object is
   *              {@link Long }
   */
  public void setUrlIdLong(Long value) {
    this.urlIdLong = value;
  }

  /**
   * Generates a String representation of the contents of this type.
   * This is an extension method, produced by the 'ts' xjc plugin
   */
  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, JAXBToStringStyle.DEFAULT_STYLE);
  }

}
