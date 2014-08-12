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

package controllers

import play.api.mvc.{AnyContent, Action, Controller}
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import common.Util._
import play.api.libs.json.{JsValue, Writes, Json}

object Broker extends Controller {

  implicit object BrokerWrites extends Writes[List[(String, Map[String, Any])]] {
    def writes(l: List[(String, Map[String, Any])]): JsValue = {
      val brokers = l.map { i =>

        val fields = i._2.map { kv =>
          kv._2 match {
            case v: Double => (kv._1, v.toInt.toString)
            case _ => (kv._1, kv._2.toString)
          }
        }

        fields + ("zookeeper" -> i._1)
      }
      Json.toJson(brokers)
    }
  }

  def index: Action[AnyContent] = Action.async {

    val brokers = connectedZookeepers { (zk, zkClient) =>

      for {
        brokerIds <- getZChildren(zkClient, "/brokers/ids/*")

        brokers <- Future.sequence(brokerIds.map(brokerId => twitterToScalaFuture(brokerId.getData())))
      } yield brokers.map(b => (zk.cluster, scala.util.parsing.json.JSON.parseFull(new String(b.bytes)).get.asInstanceOf[Map[String, Any]]))

    }

    Future.sequence(brokers).map(l => Ok(Json.toJson(l.flatten)))
  }
}
