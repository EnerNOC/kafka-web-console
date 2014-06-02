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

package models

import org.squeryl.PrimitiveTypeMode._
import org.squeryl.Schema

object Database extends Schema {
  val zookeepersTable = table[Zookeeper]("zookeepers")
  val groupsTable = table[Group]("groups")
  val statusTable = table[Status]("status")
  val consumptionDataTable = table[ConsumptionData]("consumptionData")

  val groupToZookeepers = oneToManyRelation(groupsTable, zookeepersTable).via((group, zk) => group.id === zk.groupId)
  val statusToZookeepers = oneToManyRelation(statusTable, zookeepersTable).via((status, zk) => status.id === zk.statusId)

  on(this.zookeepersTable) {
    zookeeper =>
      declare(
        zookeeper.id is primaryKey
      )
  }

  on(this.groupsTable) {
    group =>
      declare(
        group.id is autoIncremented,
        group.name is unique
      )
  }

  on(this.statusTable) {
    status =>
      declare(
        status.id is autoIncremented,
        status.name is unique
      )
  }

  on(this.consumptionDataTable) {
    consumptionData =>
      declare(
        consumptionData.id is (primaryKey, autoIncremented)
      )
  }
}
