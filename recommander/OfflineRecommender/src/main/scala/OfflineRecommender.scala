
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

// analyze data , use ^ to split
//2^                  ID
// Jumanji (1995)^    Name
// ^104 minutes^     time long
// April 30, 1997^   on line date
// 1995^             project year
// English|Français ^ Language
// Adventure|Children|Fantasy ^
// Robin Williams|Jonathan Hyde|Kirsten Dunst|Bradley Pierce|Bonnie Hunt|Bebe Neuwirth|David Alan Grier|Patricia Clarkson|Adam Hann-Byrd|Laura Bell Bundy|James Handy|Gillian Barber|Brandon Obray|Cyrus Thiedeke|Gary Joseph Thorup|Leonard Zola|Lloyd Berry|Malcolm Stewart|Annabel Kershaw|Darryl Henriques|Robyn Driscoll|Peter Bryant|Sarah Gilson|Florica Vlad|June Lion|Brenda Lockmuller|Robin Williams|Jonathan Hyde|Kirsten Dunst|Bradley Pierce|Bonnie Hunt ^Joe Johnston
case class Movie(val mid:Int, val name:String,val describe:String,val timelong:String, val issue:String,
                 val shoot:String, val language:String, val genres:String, val actors:String, val directors:String)



/***
  * Rating dataset
  * 1,
  * 1029,
  * 3.0,
  * 1260759179
  */
case class MovieRating(val uid:Int,val mid:Int, val score:Double, val timestamp:Int)
/**
  * MongoDB config
  * @param uri
  * @param db
  */
case class MongoConfig(val uri:String, val db:String)



case class Recommendation(rid:Int, r:Double)

case class UserRecs(uid:Int, recs:Seq[Recommendation])

case class MovieRecs(mid:Int, recs:Seq[Recommendation])




object OfflineRecommender {

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  val ES_MOVIE_INDEX = "Movie"


  def main(args: Array[String]): Unit = {

    // SparkConf
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )


    // create spark session
    val sparkconf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
      .set("spark.executor.memory","1G")
      .set("spark.driver.memory","1G")
    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()

    val mongoConfig = new MongoConfig(config("mongo.uri"),config("mongo.db"))
    // read mongodb


    import sparkSession.implicits._



    val ratingRDD = sparkSession
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid,rating.mid,rating.score))

    //
    val trainData = ratingRDD.map(x  => Rating(x._1,x._2,x._3))
    val (rank,iterations,lambda) = (50,10,0.01)
    // ALS model
    val model = ALS.train(trainData,rank,iterations,lambda)


    // need  usersProducts  RDD[(int),(int)]
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = sparkSession
        .read.option("uri",mongoConfig.uri)
        .option("collection",MONGODB_MOVIE_COLLECTION)
        .option("spark.mongodb.input.partitioner","MongoPaginateByCountPartitioner")  // with partition parameter
        .format("com.mongodb.spark.sql")
        .load()
        .as[Movie]
        .rdd
        .map(_.mid)

    val userMovies = userRDD.cartesian(movieRDD)
    val preRatings = model.predict(userMovies)

    val USER_MAX_RECOMMENDATION = 20
    val userRecs =  preRatings.map(rating => (rating.user, (rating.product,rating.rating)))
        .groupByKey()
        .map{
          case (uid,recs) => UserRecs(uid,recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)))
        }.toDF()

    val USER_RECS = "userRecs"
    userRecs.write
        .option("uri",mongoConfig.uri)
        .option("collection",USER_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()


    //  user recommend
    //model.predict()

    //
    sparkSession.close()
  }
}
