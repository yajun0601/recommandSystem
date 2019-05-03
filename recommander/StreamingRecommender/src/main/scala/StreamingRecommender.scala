import com.mongodb.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoClient
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("192.168.1.11")
  lazy val mongoClient = MongoClient("mongodb://localhost:27017/recommender")
}
/**
  * MongoDB config
  * @param uri
  * @param db
  */
case class MongoConfig(val uri:String, val db:String)

/**
  *
  * @param rid
  * @param r
  */
case class Recommendation(rid:Int, r:Double)

/**
  *
  * @param uid
  * @param recs
  */
case class UserRecs(uid:Int, recs:Seq[Recommendation])

/**
  *
  * @param mid
  * @param recs
  */
case class MovieRecs(mid:Int, recs:Seq[Recommendation])


object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender",
    "mongo.db" -> "recommender",
    "kafka.topic" -> "recommender"

  )

  val mongoConfig = new MongoConfig(config("mongo.uri"),config("mongo.db"))

  def main(args: Array[String]): Unit = {

      // create spark conf
      // SparkConf


    val sparkConf = new SparkConf().setAppName("StreamingRcommender").setMaster(config("spark.cores"))
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import sparkSession.implicits._

    //  broadcast movie sim matrix  =>Map[Int, Map[Int,Double]]
    val simMovieMatrix = sparkSession.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_RECS_COLLECTION)
      .option("spark.mongodb.input.partitioner","MongoPaginateByCountPartitioner")  // with partition parameter
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{recs =>
        (recs.mid,recs.recs.map(x => (x.rid,x.r)).toMap)
      }.collectAsMap()
    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)


    val kafkaPara = Map(
      "bootstrap.servers" -> "192.168.1.11:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaPara))

    // UID|MID|SCORE|TIMESTAMP  => rating stream
    val ratingStream = kafkaStream.map{ case msg =>
        val attr = msg.value().split("\\|")
      println(attr.toString)
      //if (attr.length == 4){
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)   //java.lang.NumberFormatException: For input string
      //}
    }
    println(ratingStream.count().toString)
    // create spark session     1|12|4.0|133456789
    ratingStream.foreachRDD{rdd =>
      rdd.map{
        case (uid,mid,score,timestamp) =>
          println(">>>>>>>>>>>",uid,mid,score,timestamp)
        //  get latest M times  scores which in redis
        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)

        // get movie P  sim movies number K
        val simMovies=getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMovieMatrixBroadCast.value)

        // calc recomendation priority
        val streamRecs = computeMovieScores(simMovieMatrixBroadCast.value, userRecentlyRatings, simMovies)
        saveRecsToMongoDB(uid, streamRecs)


      }.count() // action to execute
    }


    // create  kafka connection
    ssc.start()
    ssc.awaitTermination()
    // start streaming app
  }

  /**
    * save data into mongodb   uid-> 1, recs-> 11:4.5|12:4.8|13:6.0
    * @param uid
    * @param stremRecs
    */
  def saveRecsToMongoDB(uid:Int, streamRecs:Array[(Int, Double)]):Unit={
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_MOVIE_RECS_COLLECTION)
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    streamRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x => x._1 +":"+x._2).mkString("|")))
  }

  /**
    *  计算待选电影的推荐分数
    * @param simMovies    电影相似度矩阵
    * @param userRecentlyRatings  最近的K次评分
    * @param topSimMovies   用户当前电影的最相似的K个电影
    * @return
    */
  def computeMovieScores(simMovies:scala.collection.Map[Int,scala.collection.Map[Int,Double]],userRecentlyRatings:Array[(Int,Double)] ,topSimMovies:Array[Int]): Array[(Int,Double)] = {
    // to store every movie's weight score
    val score = scala.collection.mutable.ArrayBuffer[(Int,Double)]()
    // to store movies' 增强因子
    val increMap = scala.collection.mutable.HashMap[Int,Int]()

    // to store movies' 减弱因子
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()

    for(topSimMovie <- topSimMovies; userRecentlyRating<- userRecentlyRatings){
      val simScore = getMoviesSimScore(simMovies,userRecentlyRating._1,topSimMovie)
      if(simScore > 0.6){

        score += ((topSimMovie, simScore * userRecentlyRating._2 ))

        if(userRecentlyRating._2 > 3){
          increMap(topSimMovie) = increMap.getOrElse(topSimMovie, 0) + 1
        }else{
          decreMap(topSimMovie) = decreMap.getOrElse(topSimMovie, 0) + 1
        }
      }
    }

    score.groupBy(_._1).map{case (mid,similarity) =>
      (mid,similarity.map(_._2).sum / similarity.length  + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray

  }

  /**
    * log to 2
    * @param m
    * @return
    */
  def log(m:Int):Double = {
    math.log(m) / math.log(2)
  }

  /**
    *
    * @param simMovies  sim matrix
    * @param userRatingMovie
    * @param topSimMovie
    * @return
    */
  def getMoviesSimScore(simMovies:scala.collection.Map[Int,scala.collection.Map[Int,Double]], userRatingMovie:Int, topSimMovie:Int):Double = {
    simMovies.get(topSimMovie) match {
      case None => 0.0
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
    }
  }

  /**
    * return num simMovies to current movie
    * @param num
    * @param mid
    * @param uid
    * @param simMovies
    * @return
    */
  def getTopSimMovies(num:Int, mid:Int, uid:Int, simMovies:scala.collection.Map[Int,scala.collection.Map[Int,Double]]):Array[Int] = {
    // 从 广播变量 的电影相似度矩阵中获得当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray
    // 获取用户已经观看过的电影 － 就不用推荐了
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map{
      item =>
        item.get("mid").toString.toInt
    }
    // filter rated movies and output ordered list
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)


  }

  /**
    * get latest M times  scores,which in redis
    * @param num of scores
    * @param uid user id
    *
    * @return
    */
  def getUserRecentlyRating(num:Int, uid:Int, jedis:Jedis): Array[(Int,Double)] = {
    // read num scores from user redis
    jedis.lrange("uid:"+uid.toString,0,num).map{item =>
      val attr = item.split("\\:")
      println("redis:"+ attr.toString

      )
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray

  }
}
