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

object Zookeeper {

  import Database.zookeepersTable

  implicit object ZookeeperWrites extends Writes[Zookeeper] {
    def writes(zookeeper: Zookeeper): JsObject = {

      Json.obj(
        "id" -> zookeeper.id,
        "host" -> zookeeper.host,
        "port" -> zookeeper.port,
        "cluster" -> zookeeper.cluster,
        "group" -> Group.apply(zookeeper.groupId.toInt).toString,
        "status" -> Status.apply(zookeeper.statusId.toInt).toString,
        "chroot" -> zookeeper.chroot
      )
    }
  }

  def findAll: Iterable[Zookeeper] = inTransaction {
    from(zookeepersTable) {
      zk => select(zk)
    }.toList
  }

  def findByCluster(cluster: String): Iterable[Zookeeper] = inTransaction {
    from(zookeepersTable)(zk => where(zk.cluster === cluster) select zk).toList
  }

  def findByStatusId(statusId: Long): Iterable[Zookeeper] = inTransaction {
    from(zookeepersTable)(zk => where(zk.statusId === statusId) select zk).toList
  }

  def findById(id: String): Option[Zookeeper] = inTransaction {
    zookeepersTable.lookup(id)
  }

  def upsert(zookeeper: Zookeeper): Unit = inTransaction {
    val zkCount = from(zookeepersTable)(z => where(zookeeper.id === z.id) select z).toList.size
    zkCount match {
      case 1 => this.update(zookeeper)
      case _ if zkCount < 1 => this.insert(zookeeper)
      case _ =>
    }
  }

  def insert(zookeeper: Zookeeper): Zookeeper = inTransaction {
    zookeepersTable.insert(zookeeper)
  }

  def update(zookeeper: Zookeeper): Unit = inTransaction {
    zookeepersTable.update(zookeeper)
  }

  def delete(zookeeper: Zookeeper): Boolean = inTransaction {
    zookeepersTable.delete(zookeeper.id)
  }

  def update(zookeepers: Iterable[Zookeeper]) {
    inTransaction {
      zookeepersTable.update(zookeepers)
    }
  }
}

case class Zookeeper(host: String, port: Int, cluster: String, groupId: Long, statusId: Long, chroot: String)
  extends KeyedEntity[String] {

  val id = cluster + "-" + host

  override def toString: String = "%s:%s/%s".format(host, port, chroot)
}
