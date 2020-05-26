import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 * <brokers> is a list of one or more Kafka brokers
 * <groupId> is a consumer group name to consume from topics
 * <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 * $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 * consumer-group topic1,topic2
 */
object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {


    // val Array(brokers, topics) = args
    var brokers = "localhost:9092"
    var topic = "bigdata";
    var groupId = "group 1"

    // Create context with 2 second batch interval
    //val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local")
    //val ssc = new StreamingContext(sparkConf, Seconds(5))
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))


    // Create direct kafka stream with brokers and topics
    val topicsSet = topic.split(",").toSet

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //     Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    print("Mangal  kumar pandey1\n")
    //  messages.print()

    // val words = lines.flatMap(_.split(" "))
    // val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    // lines.print()
    print("Mangal  kumar pandey2")


    case class vmstat(r: String, b: String, swpd: String,
                      free: String, buff: String, cache: String,
                      si: String, so: String, bi: String,
                      bo: String, ins: String, cs: String,
                      us: String, sy: String, id: String,
                      wa: String)
    var schema = StructType(Array(StructField("r", StringType, nullable = true), StructField("b", StringType), StructField("swpd", StringType),
      StructField("free", StringType), StructField("buff", StringType), StructField("cache", StringType),
      StructField("si", StringType), StructField("so", StringType), StructField("bi", StringType),
      StructField("bo", StringType), StructField("ins", StringType), StructField("cs", StringType),
      StructField("us", StringType), StructField("sy", StringType), StructField("id", StringType),
      StructField("wa", StringType)))
    lines.foreachRDD { rdd => {
      // print(rdd.filter(line => !line.contains("memory")).collect()) rdd.foreach(println)

      var myDataLine = rdd.filter(line => !line.contains("memory"))
        .filter(line => line.length > 10)
        .filter(line => !line.contains("buff"))
        .filter(line => !line.contains("Time"))
        .map(line => line.split("[\\s]+"))
        .filter(line => line.length > 10)

      var myData = myDataLine.map(c => Row(c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12), c(13),
        c(14), c(15), c(16)))

      var df = spark.createDataFrame(myData, schema);

      df.show()

    }
    }

    def calculate(c: Array[String]): Seq[String] = {
      if (c.length > 10) {
        var data = Seq(c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12), c(13),
          c(14), c(15), c(16))
        return data
      } else {
        //  print("Hare Krishna "+sc.parallelize(c).collect())
        return Seq("Mangal", "Pandey", "SDf", "DSfsdf", "Mangal", "Pandey", "SDf", "DSfsdf", "Mangal", "Pandey", "SDf", "DSfsdf", "Mangal", "Pandey", "SDf", "DSfsdf")
      }

    }

    /*val columns = Seq("language","users_count")

    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)
    val dfFromRDD1 = rdd.toDF("language","users")
    dfFromRDD1.show()*/
    case class Uses(language: String, count: Int)
    /*  val data = Array(
        Uses("Java", 40000),
        Uses("Python", 5000)
      )
      val dogRDD = sc.parallelize(data)
      dogRDD.toDF().show();
  */
    ssc.start()
    ssc.awaitTermination()
  }
}