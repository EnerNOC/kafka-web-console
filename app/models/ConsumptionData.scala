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

import org.squeryl.KeyedEntity
import org.squeryl.PrimitiveTypeMode._
import play.api.libs.json._
import scala.collection.Iterable

object ConsumptionData {

  import Database.consumptionDataTable

  implicit object ConsumptionDataWrites extends Writes[ConsumptionData] {
    def writes(consumptionData: ConsumptionData): JsObject = {
      Json.obj(
        "factor" -> consumptionData.factor.toString,
        "timestamp" -> consumptionData.timestmp,
        "name" -> consumptionData.name,
        "offset" -> consumptionData.offsets
      )
    }
  }

  def findAll: Iterable[ConsumptionData] = inTransaction {
    from(consumptionDataTable) {
      cd => select(cd)
    }.toList
  }

  def findByTopicAndCluster(cluster: String, topic: String): Iterable[ConsumptionData] = inTransaction {
    from(consumptionDataTable)(cd => where((cd.topic === topic) and (cd.cluster === cluster)) select cd).toList
  }

  def findByNameAndCluster(name: String): Iterable[ConsumptionData] = inTransaction {
    from(consumptionDataTable)(cd => where(cd.name === name) select cd).toList
  }

  def findBeforeTime(time: Long): Iterable[ConsumptionData] = inTransaction {
    from(consumptionDataTable)(cd => where(cd.timestmp lte time) select cd).toList
  }

  def findBaseInTimeRange(timeStart: Long, timeEnd: Long): Iterable[ConsumptionData] = inTransaction {
    from(consumptionDataTable)(cd => where((cd.timestmp gte timeStart) and (cd.timestmp lte timeEnd) and (cd.factor === 1L)) select cd).toList
  }

  def findByFactor(factor: Long): Iterable[ConsumptionData] = inTransaction {
    from(consumptionDataTable)(cd => where(cd.factor === factor) select cd).toList
  }

  def upsert(consumptionData: ConsumptionData): Unit = inTransaction {
    val cdCount = from(consumptionDataTable)(cd => where(consumptionData.timestmp === cd.timestmp) select cd).toList.size
    cdCount match {
      case 1 => this.update(consumptionData)
      case _ if cdCount < 1 => this.insert(consumptionData)
      case _ =>
    }
  }

  def insert(consumptionData: ConsumptionData): ConsumptionData = inTransaction {
    consumptionDataTable.insert(consumptionData)
  }

  def update(consumptionData: ConsumptionData): Unit = inTransaction {
    consumptionDataTable.update(consumptionData)
  }

  def delete(consumptionData: ConsumptionData): Boolean = inTransaction {
    consumptionDataTable.delete(consumptionData.id)
  }

  def update(consumptionData: Iterable[ConsumptionData]) {
    inTransaction {
      consumptionDataTable.update(consumptionData)
    }
  }
}

case class ConsumptionData(factor: Long, timestmp: Long, name: String, cluster: String, topic: String, offsets: Long) extends KeyedEntity[Long] {
  val id: Long = 0
}
