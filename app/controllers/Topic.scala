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

import play.api.mvc.{AnyContent, WebSocket, Action, Controller}
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.libs.json.JsObject
import play.api.libs.iteratee.{Concurrent, Iteratee}
import models.Cache

object Topic extends Controller {

  /**
   * Defines the format for JSON when sending the list of topics.
   */
  object TopicsWrites extends Writes[Iterable[(String, Iterable[Int], String)]] {
    def writes(l: Iterable[(String, Iterable[Int], String)]): JsArray = {
      val topics = l.map(tp => {
        JsObject(Seq("name" -> Json.toJson(tp._1), "partitions" -> Json.toJson(tp._2), "cluster" -> Json.toJson(tp._3)))
      }).toSeq
      JsArray(topics)
    }
  }

  /**
   * Defines the format for JSON when sending an individual topic.
   */
  object TopicWrites extends Writes[List[Map[String, Object]]] {
    def writes(l: List[Map[String, Object]]): JsArray = {
      val topics = l.map { t =>

        val js = t.map { e =>
          val v = e._2 match {
            case v: Seq[_] =>
              Json.toJson(e._2.asInstanceOf[Seq[Map[String, String]]])
            // TODO: There is a warning here
            case v => Json.toJson(v.toString)
          }
          (e._1, v)
        }.toList

        JsObject(js)
      }
      JsArray(topics)
    }
  }

  /**
   * Main page for topics.
   * @return Topics to display.
   */
  def index: Action[AnyContent] = Action.async {
    val data = Cache.queryClustersAndTopics()
    Future(Ok(Json.toJson(data)(TopicsWrites)))
  }

  /**
   * View for a topic and related consumer groups.
   * @param topic Topic name.
   * @param cluster Zookeeper cluster name.
   * @return Consumer groups and offsets.
   */
  def show(topic: String, cluster: String): Action[AnyContent] = Action.async {
    val data = collectPartitions(Cache.queryOffsets(cluster, topic))
    Future(Ok(Json.toJson(data)(TopicWrites)))
  }

  /**
   * Collects the starting and ending offsets for a topic.
   * @param topic Topic name.
   * @param cluster Zookeeper cluster name.
   * @return Starting and ending offsets.
   */
  def range(topic: String, cluster: String): Action[AnyContent] = Action.async {
    val data = collectPartitions(Cache.queryRanges(cluster, topic))
    Future(Ok(Json.toJson(data)(TopicWrites)))
  }

  /**
   * Samples messages from each partition of a topic.
   * @param topic Topic name.
   * @param cluster Zookeeper cluster name.
   * @return Message sample set.
   */
  def feed(topic: String, cluster: String): Action[AnyContent] = Action.async {
    Future(Ok(Json.toJson(Cache.queryMessages(cluster, topic))))
  }

  /**
   * Feeds a set of data to the viewer so that the graph can be displayed.
   * @param topic Topic name.
   * @param cluster Zookeeper cluster name.
   * @return Set of data points.
   */
  def graph(topic: String, cluster: String): WebSocket[String] = WebSocket.using[String] { implicit request =>

    val id = new scala.util.Random().nextInt()

    val out = Concurrent.unicast[String] { channel: Concurrent.Channel[String] =>
      play.api.Logger.info("Web client connected to graph.")
      val cb = (cd: models.ConsumptionData) => {
        channel.push(Json.toJson(cd).toString())
      }

      val topicData = models.ConsumptionData.findByTopicAndCluster(cluster, topic)
      topicData.map(d => {
        cb(d)
      })
      actors.DataCollector.addSocket(id, topic, cb)
    }

    val in = Iteratee.foreach[String](println).map { _ =>
      play.api.Logger.info("Web client disconnected from graph.")
      actors.DataCollector.removeSocket(id)
    }

    (in, out)
  }

  /**
   * Used to collect and format a sequence of consumer and topic offsets.
   * @param sequence Set of (consumerGroup, partition, offset)
   * @return Collection of consumerGroup data.
   */
  def collectPartitions(sequence: Seq[(String, String, String)]): List[Map[String, Object]] = {
    sequence.filter((a: (String, String, String)) => a._1 != "").groupBy(_._1).map { s =>
      Map("consumerGroup" -> s._1, "offsets" -> s._2.map { t =>
        Map("partition" -> t._2, "offset" -> t._3)
      })
    }.toList
  }
}
