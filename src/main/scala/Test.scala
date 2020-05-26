

object Test {

  def main(args: Array[String]): Unit = {

/*

    val (brokerlist, topics) = ("localhost:9092", "bigdata")
    val topicMap = topics.split(",").toSet
    var topic = "bigdata"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    import spark.implicits._

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.map(_.toString).toset()).map(_._2)
    case class vmstat(r: String, b: String, swpd: String,
                      free: String, buff: String, cache: String,
                      si: String, so: String, bi: String,
                      bo: String, ins: String, cs: String,
                      us: String, sy: String, id: String,
                      wa: String)



    lines.foreachRDD { rdd =>
      //rdd.foreach(println)
      val mytable = rdd.filter(line => !line.contains("memory"))
        .filter(line => !line.contains("buff"))
        .map(line => line.split("[\\s]+"))
        .map(c =>
          vmstat(c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12), c(13),
            c(14), c(15), c(16)))
        .toDF()
      mytable.show(5) //.foreach(println)

      mytable.write.format("orc").mode(SaveMode.Append).saveAsTable("vmstat_kafka")
    }
    ssc.start // start your spark-streaming application with this command

*/
  }
}
