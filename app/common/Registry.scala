/**
 * Copyright (C) 2014 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
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

package common

import common.Registry.PropertyConstants.PropertyConstants

object Registry {

  object PropertyConstants extends Enumeration {
    type PropertyConstants = Value
    val ZookeeperConnections = Value("ZOOKEEPER-CONNECTIONS")
    val BroadcastChannel = Value("BROADCAST-CHANNEL")
  }

  private var properties = Map[String, Any]()

  def lookupObject(propertyName: String): Option[Any] = {
    properties.get(propertyName)
  }

  def lookupObject(propertyName: PropertyConstants): Option[Any] = {
    this.lookupObject(propertyName.toString)
  }

  def registerObject[A](name: String, value: A): A = {
    properties = properties ++ Map(name -> value)
    value
  }

  def registerObject[A](name: PropertyConstants, value: A): A = {
    this.registerObject(name.toString, value)
    value
  }

}

