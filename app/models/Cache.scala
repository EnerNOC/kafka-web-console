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

import common.Util._
import consumer.LowLevelConsumer

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import collection.JavaConversions._

/**
 * Primary object for interfacing with the cache/database.
 */
object Cache {

  private var cache = mutable.Map[String,Cluster]()

  def update(cluster: String, name: String, topic: String, partition: Int, offset: Long, timestamp: Long) {
    if(!cache.contains(cluster)) {
      cache += (cluster -> new Cluster())
    }
    cache(cluster).update(name, topic, partition, offset, timestamp)
  }

  def updateEnd(cluster: String, topic: String, partition: Int, offset: Long, timestamp: Long) {
    if(!cache.contains(cluster)) {
      cache += (cluster -> new Cluster())
    }
    cache(cluster).updateEnd(topic, partition, offset, timestamp)
  }

  def updateStart(cluster: String, topic: String, partition: Int, offset: Long, timestamp: Long) {
    if(!cache.contains(cluster)) {
      cache += (cluster -> new Cluster())
    }
    cache(cluster).updateStart(topic, partition, offset, timestamp)
  }

  def queryRanges(cluster: String, topic: String): Seq[(String, String, String)] = {
    cache(cluster).queryRanges(topic)
  }

  def queryOffsets(cluster: String, topic: String): Seq[(String, String, String)] = {
    cache(cluster).queryOffsets(topic)
  }

  def queryOffset(cluster: String, topic: String, partition: Int, name: String): (Long, Long) = {
    (cache(cluster).topics(topic).partitions(partition).consumerGroups(name).offset,
     cache(cluster).topics(topic).partitions(partition).consumerGroups(name).timestamp)
  }

  def queryEndOffset(cluster: String, topic: String, partition: Int): (Long, Long) = {
    // TODO: This shouldn't work the first time
    (cache(cluster).topics(topic).partitions(partition).end.offset,
     cache(cluster).topics(topic).partitions(partition).end.timestamp)
  }

  def queryMessages(cluster: String, topic: String): Set[String] = {
    if(!cache.contains(cluster)) {
      cache += (cluster -> new Cluster())
    }
    cache(cluster).queryMessages(cluster, topic)
  }

  def queryClustersAndTopics(): Iterable[(String, Iterable[Int], String)] = {
    cache.keys.map(c => {
      cache(c).topics.keys.map(t => {
          (t, cache(c).topics(t).partitions.keys, c)
        })
      }
    ).flatten
  }

  def queryConsumergroup(cluster: String, name: String): Seq[(String, Int, Long, Long)] = {
    cache(cluster).topics.map(t => {
      t._2.partitions.map(p => {
        if(p._2.consumerGroups.contains(name)) {
          (t._1, p._1, p._2.end.offset, p._2.consumerGroups(name).offset)
        } else {
          ("", 0, 0L, 0L)
        }
      }).filter(p => p._1 != "")
    }).flatten.toSeq
  }
}

class Cluster() {
  var topics = mutable.Map[String,Topic]()

  def update(name: String, topic: String, partition: Int, offset: Long, timestamp: Long) {
    if(!topics.contains(topic)) {
      topics += (topic -> new Topic())
    }
    topics(topic).update(name, partition, offset, timestamp)
  }

  def updateEnd(topic: String, partition: Int, offset: Long, timestamp: Long) {
    if(!topics.contains(topic)) {
      topics += (topic -> new Topic())
    }
    topics(topic).updateEnd(partition, offset, timestamp)
  }

  def updateStart(topic: String, partition: Int, offset: Long, timestamp: Long) {
    if(!topics.contains(topic)) {
      topics += (topic -> new Topic())
    }
    topics(topic).updateStart(partition, offset, timestamp)
  }

  def queryRanges(topic: String): Seq[(String, String, String)] = {
    topics(topic).queryRanges()
  }

  def queryOffsets(topic: String): Seq[(String, String, String)] = {
    topics(topic).queryOffsets()
  }

  def queryMessages(cluster: String, topic: String): Set[String] = {
    if(!topics.contains(topic)) {
      topics += (topic -> new Topic())
    }
    topics(topic).queryMessages(cluster, topic)
  }
}

class Topic() {
  var partitions = mutable.Map[Int,Partition]()
  private var messages = Set[String]()
  private var timestamp: Long = 0

  def update(name: String, partition: Int, offset: Long, timestamp: Long) {
    if(!partitions.contains(partition)) {
      partitions += (partition -> new Partition())
    }
    partitions(partition).update(name, offset, timestamp)
  }

  def updateEnd(partition: Int, offset: Long, timestamp: Long) {
    if(!partitions.contains(partition)) {
      partitions += (partition -> new Partition())
    }
    partitions(partition).updateEnd(offset, timestamp)
  }

  def updateStart(partition: Int, offset: Long, timestamp: Long) {
    if(!partitions.contains(partition)) {
      partitions += (partition -> new Partition())
    }
    partitions(partition).updateStart(offset, timestamp)
  }

  def queryRanges(): Seq[(String, String, String)] = {
    partitions.map(s => {
      Set(("Start", s._1.toString, s._2.start.offset.toString), ("End", s._1.toString, s._2.end.offset.toString))
    }).flatten.toSeq
  }

  def queryOffsets(): Seq[(String, String, String)] = {
    partitions.map(s => {
      s._2.consumerGroups.map(c => {
        (c._1, s._1.toString, c._2.offset.toString)
      })
    }).flatten.toSeq
  }

  def queryMessages(cluster: String, topic: String): Set[String] = {
    if(timestamp < System.currentTimeMillis() - 10*1000) {
      val connectedZks = connectedZookeepers((z, c) => (z, c)).filter(_._1.cluster == cluster)

      if (connectedZks.size > 0) {
        val (_, zkClient) = connectedZks.head
        val (address, port) = getSeedBroker(zkClient)

        val partitionNodes = Await.result(getZChildren(zkClient, "/brokers/topics/" + topic + "/partitions/*"), Duration.Inf).toSet
        val messages = partitionNodes.map(n => n.path.split("/").filter(_ != "")(4)).map(p => {
          // TODO: Make number a config
          val consumer = new LowLevelConsumer(topic, p.toInt, address, port)
          val messages = consumer.retrieveData(10)
          consumer.closeConsumers()

          messages
        })

        this.messages = messages.flatten
      } else {
        this.messages = Set[String]()
      }
      timestamp = System.currentTimeMillis()
    }

    messages
  }
}

class Partition() {
  var start: ConsumerGroup = new ConsumerGroup()
  var end: ConsumerGroup = null
  var consumerGroups = mutable.Map[String,ConsumerGroup]()

  def update(name: String, offset: Long, timestamp: Long) {
    if(!consumerGroups.contains(name)) {
      consumerGroups += (name -> new ConsumerGroup())
    }
    consumerGroups(name).update(offset, timestamp)
  }

  def updateEnd(offset: Long, timestamp: Long) {
    if(end == null) {
      end = new ConsumerGroup()
    }
    end.update(offset, timestamp)
  }

  def updateStart(offset: Long, timestamp: Long) {
    start.update(offset, timestamp)
  }
}

class ConsumerGroup() {
  var offset: Long = 0
  var timestamp: Long = 0

  def update(offset: Long, timestamp: Long) {
    this.offset = offset
    this.timestamp = timestamp
  }
}
