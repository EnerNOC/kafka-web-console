name := "kafka-web-console"

version := "1.1.1-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  "org.squeryl" % "squeryl_2.10" % "0.9.5-6",
  "com.twitter" % "util-zk_2.10" % "6.11.0",
  "org.apache.kafka" % "kafka_2.10" % "0.8.1"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)

scalacOptions += "-feature"

play.Project.playScalaSettings

org.scalastyle.sbt.ScalastylePlugin.Settings
