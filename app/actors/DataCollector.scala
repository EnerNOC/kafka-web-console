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

package actors

import akka.actor.Actor
import com.twitter.util.Await
import consumer.LowLevelConsumer
import common.Util._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import models._
import scala.collection.mutable
import com.twitter.zk.ZkClient
import scala.util.parsing.json.JSON

object DataCollector extends Actor {

  private var socketList = mutable.Map[Int,(String,(ConsumptionData) => Unit)]()

  def receive: PartialFunction[Any, Unit] = {

    case Record() =>
      recordConsumerEntries()
      recordTopicEntries()

    case Clean() =>
//      val duration = 1000*60*60*24*3 //3 days
      val duration = 1000*60*60*6*1 //6 hours
      val timestamp = System.currentTimeMillis
      ConsumptionData.findBeforeTime(timestamp - duration).foreach(cd => {
        ConsumptionData.delete(cd)
      })

    case Aggregate(factor: Int, base: Int) =>
      aggregateAndStore(factor, base)
  }

  /**
   * Collects and records topic offset data to the database and cache.
   */
  private def recordTopicEntries() {
    val connectedZks = connectedZookeepers((z, c) => (z, c))

    connectedZks.foreach(zkInfo => {
      val (zk, zkClient) = zkInfo
      val (address, port) = getSeedBroker(zkClient)
      getZChildren(zkClient, "/brokers/topics/*/partitions/*").map(s =>
        s.map(p => (p.path.split("/").filter(_ != "")(2), p.path.split("/").filter(_ != "")(4))).groupBy(zn => zn._1).foreach(t => {
          var rate: Long = 0
          var success = false
          t._2.foreach(p => {
            val topic = p._1
            val partition = p._2.toInt
            val cluster = zk.cluster

            val consumer = new LowLevelConsumer(topic, partition, address, port)
            val timestamp = System.currentTimeMillis
            val endingOffset = consumer.endingOffset()

            try {
              val (lOffset, lTimestamp) = Cache.queryEndOffset(cluster, topic, partition)
              rate += ((endingOffset - lOffset) * 1000) / (timestamp - lTimestamp)
              success = true
            } catch {
              case e: Exception => play.api.Logger.debug("Cache empty! - " + topic)
            }
            Cache.updateEnd(cluster, topic, partition, endingOffset, timestamp)

            val startingOffset = consumer.startingOffset()
            storeDataStart(zk.cluster, p._1, p._2.toInt, startingOffset, timestamp)

            consumer.closeConsumers()
          })

          if(success) {
            val timestamp = System.currentTimeMillis()
            val topic = t._1
            pushUpdate(topic, new ConsumptionData(1, timestamp, topic, zk.cluster, topic, rate))
          }
        })
      )
    })
  }

  /**
   * Collects and records consumer offset data to the database and cache.
   */
  private def recordConsumerEntries() {
    val connectedZks = connectedZookeepers((z, c) => (z, c))

    connectedZks.foreach(zkInfo => {
      val (zk, zkClient) = zkInfo

      recordStormSpoutConsumers(zk.cluster, zkClient)
      recordHighLevelConsumers(zk.cluster, zkClient)
    })
  }

  /**
   * Records data entries for kafka high level consumers.
   * @param cluster Zookeeper cluster.
   * @param zkClient Zookeeper connection interface.
   */
  private def recordHighLevelConsumers(cluster: String, zkClient: ZkClient) {
    getZChildren(zkClient, "/consumers/*/offsets/*/*").map(s =>
      s.groupBy(zn => zn.path.split("/").filter(_ != "")(3)).foreach(seq => {
        var rate: Long = 0
        var success = false

        seq._2.foreach(p =>
          p.getData().map(d => {
            val timestamp = System.currentTimeMillis
            val name = p.path.split("/").filter(_ != "")(1)
            val topic = p.path.split("/").filter(_ != "")(3)
            val partition = p.name.toInt
            val offset = new String(d.bytes).toLong

            try {
              val (lOffset, lTimestamp) = Cache.queryOffset(cluster, topic, partition, name)
              rate += ((offset - lOffset) * 1000) / (timestamp - lTimestamp)
              success = true
            } catch {
              case e: Exception => play.api.Logger.debug("Cache empty! - " + name)
            }
            Cache.update(cluster, name, topic, partition, offset, timestamp)
          })
        )

        if(success) {
          val timestamp = System.currentTimeMillis()
          val topic = seq._1
          pushUpdate(topic, new ConsumptionData(1, timestamp, topic, cluster, topic, rate))
        }
      })
    )
  }

  /**
   * Records data entries for storm spout consumers.
   * @param cluster Zookeeper cluster.
   * @param zkClient Zookeeper connection interface.
   */
  private def recordStormSpoutConsumers(cluster: String, zkClient: ZkClient) {
    getZChildren(zkClient, "/*/*").map(n => n.filter(z => {
      val t = z.path.split("/").filter(_ != "")(0)
      (t != "admin") && (t != "brokers") && (t != "consumers") && (t != "controllers") && (t != "controller_epoch")
    })).map(s =>
      s.map(p =>
        Await.result(p.getData().map(d => {

          val data = new String(d.bytes)
          JSON.perThreadNumberParser = { input: String => Integer.parseInt(input)}
          JSON.parseFull(data) match {

            //TODO: This is a warning
            case Some(map: Map[String, Any]) =>
              val timestamp = System.currentTimeMillis
              val name = p.path.split("/")(1)
              val topic = map("topic").toString
              val partition = map("partition").toString.toInt
              val offset = map("offset").toString.toLong
              var rate: Long = 0
              var success = false
              try {
                val (lOffset, lTimestamp) = Cache.queryOffset(cluster, topic, partition, name)
                rate = ((offset - lOffset) * 1000) / (timestamp - lTimestamp)
                success = true
              } catch {
                case e: Exception => play.api.Logger.debug("Cache empty! - " + name)
              }
              Cache.update(cluster, name, topic, partition, offset, timestamp)
              (timestamp, name, cluster, topic, rate, success)

            case _ => throw new Exception("JSON Parse Failed")
          }
        }))
      ).groupBy(p => (p._2, p._4)).foreach(t => {
        val topic = t._1._2
        val timestamp = System.currentTimeMillis()
        val name = t._1._1

        var rate: Long = 0
        t._2.foreach(p => {
          if(p._6) {
            rate += p._5
          }
        })

        pushUpdate(topic, new ConsumptionData(1, timestamp, name, cluster, topic, rate))
      })
    )
  }

  def aggregateAndStore(factor: Long, base: Long) {
    play.api.Logger.debug("Aggregating: " + factor + ", " + base)
    val data = ConsumptionData.findBaseInTimeRange(System.currentTimeMillis() - factor*base, System.currentTimeMillis())
    data.groupBy(cd => (cd.cluster, cd.topic, cd.name)).map(a => {
      var average: Long = 0
      a._2.map(cd => cd.offsets).foreach(offset => average += offset / a._2.size)
      pushUpdate(a._1._2, new ConsumptionData(factor, System.currentTimeMillis(), a._1._3, a._1._1, a._1._2, average))
    })
  }

  /**
   * Sends data for the starting offset to the cache.
   * @param cluster Zookeeper custer name.
   * @param topic Topic name.
   * @param partition Partition number.
   * @param offset Offset of the broker.
   * @param timestamp Time at which the data was collected.
   */
  private def storeDataStart(cluster: String, topic: String, partition: Int, offset: Long, timestamp: Long) {
    Cache.updateStart(cluster, topic, partition, offset, timestamp)
  }

  /**
   * Sends data to the database and any matching websockets.
   * @param topic Topic name.
   * @param cd The data unit.
   */
  private def pushUpdate(topic: String, cd: ConsumptionData) {
    socketList.foreach(cb => {
      if(cb._2._1 == topic) {
        cb._2._2(cd)
      }
    })
    ConsumptionData.insert(cd)
  }

  /**
   * Registers a callback to an open socket.
   * @param id Identifier of the callback to allow removal.
   * @param topic Name of the topic to feed via the callback.
   * @param cb The callback function.
   */
  def addSocket(id: Int, topic: String, cb: (ConsumptionData) => Unit) {
    socketList += (id -> (topic, cb))
  }

  /**
   * Removes a callback when no longer needed.
   * @param id Identifier of the callback.
   */
  def removeSocket(id: Int) {
    socketList.remove(id)
  }
}

case class Record()

case class Clean()

case class Aggregate(factor: Int, base: Int)
