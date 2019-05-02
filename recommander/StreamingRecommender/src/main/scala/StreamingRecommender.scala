import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingRecommender {

  def main(args: Array[String]): Unit = {

      // create spark conf
      // SparkConf
      val config = Map(
        "spark.cores" -> "local[*]",
        "mongo.uri" -> "mongodb://localhost:27017/recommender",
        "mongo.db" -> "recommender"
      )

    val sparkConf = new SparkConf().setAppName("StreamingRcommender").setMaster(config("spark.cores"))
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaStream))

    // UID|MID|SCORE|TIMESTAMP  => rating stream
    val ratingStream = kafkaStream.map{ case msg =>
        val attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    // create spark session
    ratingStream.foreachRDD{rdd =>
      rdd.map{case (uid,mid,score,timestamp) => println(">>>>>>>>>>>")}.count() // action to execute
    }

    // create  kafka connection
    ssc.start()
    ssc.awaitTermination()
    // start streaming app
  }
}
