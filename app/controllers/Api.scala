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
import models.Cache
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json

object Api extends Controller {

  /**
   * Collects offset information for a specific consumerGroup.
   * @param cluster Zookeeper cluster name.
   * @param consumerGroup ConsumerGroup name.
   * @return Information on consumerGroup in JSON format.
   */
  def cgstatus(cluster: String, consumerGroup: String): Action[AnyContent] = Action.async{


    if (consumerGroup == "") {
      Future(Status(400)("Consumer group must not be empty."))

    } else if (cluster == "") {
      Future(Status(400)("Cluster must not be empty."))

    } else {
      try {
        val data = Cache.queryConsumergroup(cluster, consumerGroup)

        if(data.length > 0) {
          val response = Json.obj("consumerGroup" -> consumerGroup,
            "topics" -> data.groupBy(_._1).map(t => {
              t._1 -> t._2.groupBy(_._2).flatMap(p => {
                  p._2.map(d => {
                    Json.obj("partition" -> p._1,
                      "brokerOffset" -> d._3,
                      "consumerGroupOffset" -> d._4,
                      "offsetLag" -> (d._3 - d._4)
                    )
                  })
                }
              )
            })
          )

          Future(Ok(Json.toJson(response)))
        } else {
          Future(Status(404)("The consumer group specified could not be found."))
        }

      } catch {
        case nsee: NoSuchElementException =>
          val id = new scala.util.Random().nextInt()
          play.api.Logger.error("Exception ID: " + id)
          nsee.printStackTrace()
          Future(Status(404)("The cluster specified does not exist. Exception ID: " + id))

        case e: Exception =>
          val id = new scala.util.Random().nextInt()
          play.api.Logger.error("Exception ID: " + id)
          e.printStackTrace()
          Future(Status(500)("An error occured processing the request. Exception ID: " + id))
      }
    }
  }
}
