name := "StreamingRnD"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion="1.4.0"
val log4jVersion="1.2.14"
val kafkaVersion="0.8.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.kafka" % "kafka_2.10" % kafkaVersion,
  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0.Beta3",
  "log4j" % "log4j" % log4jVersion

)

resolvers ++= Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)

    