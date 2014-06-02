Kafka Web Console
=========
Kafka Web Console is a Java web application for monitoring [Apache Kafka](http://kafka.apache.org/). With a **modern** web browser, you can view from the console:

   - Registered brokers
   
![brokers](/img/brokers.png)

***

   - Topics and their partitions
   
![topics](/img/topics.png)

***

   - Consumer groups, individual consumers, and partition offsets for each consumer group
    
![topic](/img/topic.png)

***

   - Latest published topic messages

![topic feed](/img/topic-feed.png)

***

   - Live graph showing message consumption rates (requires web browser support for WebSocket)

![topic graph](/img/topic-graph.png)

***

   - Furthermore, the console provides a JSON API.
      * The only endpoint currently available is "consumergroupstatus", and responds only to a GET request.
      * &lt;HOSTNAME>:&lt;PORT>/api/consumergroupstatus?zookeeper=&lt;CLUSTER or ENSEMBLE>&consumergroup=&lt;CONSUMER GROUP>
      * Error code 400 is given if either the zookeeper or consumergroup is not specified.
      * Error code 404 is given if both are specified and either is unable to be found.
      * Error code 500 is given on all other errors. Additionally an exception ID is returned.
      * A successful request will respond with status code 200, and the requested data in the following JSON format:
```
{
   "consumerGroup":"<CONSUMER GROUP>",
   "topics":{
      "<FIRST TOPIC>":[
         {"partition":1,
          "brokerOffset":<INT>,
          "consumerGroupOffset":<INT>,
          "offsetLag":<INT>},
         {"partition":0,
          "brokerOffset":<INT>,
          "consumerGroupOffset":<INT>,
          "offsetLag":<INT>}
      ],
      "<SECOND TOPIC>":[
         {"partition":1,
          "brokerOffset":<INT>,
          "consumerGroupOffset":<INT>,
          "offsetLag":<INT>},
         {"partition":0,
          "brokerOffset":<INT>,
          "consumerGroupOffset":<INT>,
          "offsetLag":<INT>}
      ]
   }
}
```
        
![api](/img/api.png)

***

Requirements
---
- Play Framework 2.2.x
- Apache Kafka 0.8.x
- Zookeeper 3.3.3

Deployment
----
Consult Play!'s documentation for [deployment options and instructions](http://www.playframework.com/documentation/2.2.x/Production).

Getting Started
---
1. Kafka Web Console requires a relational database. Consult Play!'s documentation to [specify the database to be used by the console](http://www.playframework.com/documentation/2.2.x/ScalaDatabase). The following databases are supported:
   - H2
   - PostgreSql
   - Oracle
   - DB2
   - MySQL
   - Apache Derby
   - Microsoft SQL Server<br/><br/>
2. Before you can monitor a broker, you need to register the Zookeeper server associated with it:

![register zookeeper](/img/register-zookeeper.png)

Filling in the form and clicking on *Connect* will register the Zookeeper server. Once the console has successfully established a connection with the registered Zookeeper server, it can retrieve all necessary information about brokers, topics, and consumers. Connecting to multiple zookeepers in one cluster (ensemble) provides redundancy:

![zookeepers](/img/zookeepers.png)

Support
---
Please [report](http://github.com/EnerNOC/kafka-web-console/issues) any bugs or desired features.
