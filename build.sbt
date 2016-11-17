name := "SparkKafkaStreaming"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"    % "2.0.1" ,
  "org.apache.spark"  %% "spark-streaming"    % "2.0.1" ,
  "org.apache.spark"  %% "spark-streaming-kafka"  % "1.6.3",
  "org.apache.hbase" % "hbase-client" % "1.2.4",
  "org.apache.hbase" % "hbase" % "1.2.4",
  "org.apache.hbase" % "hbase-server" % "1.2.4",
  "org.apache.hbase" % "hbase-common" % "1.2.4",
  "org.apache.hbase" % "hbase-hadoop2-compat" % "1.2.4"

    exclude("org.scala-lang", "scalreaminga-reflect")
    exclude("org.scala-lang.modules", "scala-xml_2.11")

    exclude("org.scala-lang", "scala-compiler")
    exclude("org.scala-lang.modules", "scala-parser-combinators_2.11")

)
