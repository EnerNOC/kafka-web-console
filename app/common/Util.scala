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

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import com.twitter.util.{Throw, Return}
import com.twitter.zk.{ZNode, ZkClient}
import common.Registry.PropertyConstants
import models.Zookeeper
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import org.apache.zookeeper.KeeperException.NoNodeException
import scala.util.parsing.json.JSON

object Util {
  def twitterToScalaFuture[A](twitterFuture: com.twitter.util.Future[A]): Future[A] = {
    val promise = Promise[A]()
    twitterFuture respond {
      case Return(a) => promise success a
      case Throw(e) => promise failure e
    }
    promise.future
  }

  def connectedZookeepers[A](block: (Zookeeper, ZkClient) => A): List[A] = {
    val connectedZks = models.Zookeeper.findByStatusId(models.Status.Connected.id).groupBy(zk => zk.cluster).map(zks => zks._2.head)

    val zkConnections: Map[String, ZkClient] = Registry.lookupObject(PropertyConstants.ZookeeperConnections) match {
      case Some(s: Map[_, _]) if connectedZks.size > 0 => s.asInstanceOf[Map[String, ZkClient]]
      case _ => Map()
    }

    connectedZks.map(zk => block(zk, zkConnections.get(zk.id).get)).toList
  }

  def getZChildren(zkClient: ZkClient, path: String): Future[Seq[ZNode]] = {
    val nodes = path.split('/').filter(_ != "").toList

    getZChildren(zkClient("/"), nodes)
  }

  def getZChildren(zNode: ZNode, path: List[String]): Future[Seq[ZNode]] = path match {

    case head :: tail if head == "*" =>
      val subtreesFuture = for {
        children <- twitterToScalaFuture(zNode.getChildren()).map(_.children).recover {
          case e: NoNodeException => Nil
        }
        subtrees <- Future.sequence(children.map(getZChildren(_, tail)))

      } yield subtrees

      subtreesFuture.map(_.flatten)

    case head :: Nil =>
      twitterToScalaFuture(zNode(head).exists()).map(_ => Seq(zNode(head))).recover {
        case e: NoNodeException => Nil
      }

    case head :: tail => getZChildren(zNode(head), tail)
    case Nil => Future(Seq(zNode))
  }

  /**
   * Queries zookeeper to get the connection information for a broker to use as a seed broker.
   * @param zkClient Connection to zookeeper.
   * @return Broker information.
   */
  def getSeedBroker(zkClient: ZkClient): (String, Int) = {
    val brokerData = for {
      brokerIds <- getZChildren(zkClient, "/brokers/ids/*")
      brokers <- Future.sequence(brokerIds.map(brokerId => twitterToScalaFuture(brokerId.getData())))
    } yield brokers.map(b => {
        val myConversionFunc = {input : String => Integer.parseInt(input)}
        JSON.perThreadNumberParser  = myConversionFunc
        val data = scala.util.parsing.json.JSON.parseFull(new String(b.bytes)).get.asInstanceOf[Map[String, Any]]
        (data("host").asInstanceOf[String], data("port").asInstanceOf[Int])
    })
    Await.result(brokerData, Duration.Inf).head
  }
}
