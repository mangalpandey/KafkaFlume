name := "KafkaFlume"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0",


"org.apache.spark" %% "spark-sql" % "2.2.0"
)
//org.apache.spark