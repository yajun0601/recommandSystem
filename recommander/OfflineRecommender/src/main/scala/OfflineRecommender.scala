
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.jblas.DoubleMatrix

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


  val USER_MAX_RECOMMENDATION = 8

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  val ES_MOVIE_INDEX = "Movie"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  def main(args: Array[String]): Unit = {

    // Conf
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )


    // create spark config session
    val sparkconf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
      .set("spark.executor.memory","2G")
      .set("spark.driver.memory","1G")
    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()

    val mongoConfig = new MongoConfig(config("mongo.uri"),config("mongo.db"))
    // read mongodb


    import sparkSession.implicits._

    // TRAIN ALS MODLE

    val ratingRDD = sparkSession
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid,rating.mid,rating.score))
      .cache() // cache result, not calculate again

    // create train data set
    val trainData = ratingRDD.map(x  => Rating(x._1,x._2,x._3))
    val (rank,iterations,lambda) = (30,10,0.01)
    // ALS model
    val model = ALS.train(trainData,rank,iterations,lambda)

    model.save(sparkSession.sparkContext,"als.model")
//    model.predict()

    // need  usersProducts  RDD[(int),(int)]
    val userRDD = ratingRDD.map(_._1).distinct().cache()
    val movieRDD = sparkSession
        .read.option("uri",mongoConfig.uri)
        .option("collection",MONGODB_MOVIE_COLLECTION)
        .option("spark.mongodb.input.partitioner","MongoPaginateByCountPartitioner")  // with partition parameter
        .format("com.mongodb.spark.sql")
        .load()
        .as[Movie]
        .rdd
        .map(_.mid)
      .cache() // cache result, not calculate again

    val userMovies = userRDD.cartesian(movieRDD)
    val preRatings = model.predict(userMovies)
    val userRecs =  preRatings.map(rating => (rating.user, (rating.product,rating.rating)))
        .groupByKey()
        .map{
          case (uid,recs) => UserRecs(uid,recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)))
        }.toDF()

    userRecs.write
        .option("uri",mongoConfig.uri)
        .option("collection",USER_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()


    //  user recommend
    //model.predict()
    val movieFeatures = model.productFeatures.map{case (mid,features) =>
      (mid, new DoubleMatrix(features))}

    val movieRecs =  movieFeatures.cartesian(movieFeatures)
        .filter{case (a,b) => a._1 != b._1} // movieA.id != movieB.id
        .map{ case (a,b) =>
        val simScore = this.cosinSim(a._2,b._2)
      (a._1,(b._1,simScore))
      }.filter(_._2._2 > 0.8)
      .groupByKey()
      .map{
        case (mid,items) => MovieRecs(mid,items.toList.map(x => Recommendation(x._1,x._2)))
      }.toDF()

    movieRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //
    sparkSession.close()
  }
  // cosin similarity of two movies
  def cosinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2)/(movie1.norm2() * movie2.norm2())
  }
}
