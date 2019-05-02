import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession




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
case class Rating(val uid:Int,val mid:Int, val score:Double, val timestamp:Int)
/**
  * Tag data set
  * 15,
  * 339,
  * sandra 'boring' bullock, tag
  * 1138537770                timestamp
  * */
case class Tag(val uid:Int, val mid:Int, val tag:String, val timestamp:Int)

/**
  * MongoDB config
  * @param uri
  * @param db
  */
case class MongoConfig(val uri:String, val db:String)

/**
  *
  * @param httpHosts  ES host 以，分割
  * @param trasportHosts 主机列表，以，分割
  * @param index
  * @param clustername  集群名称
  */
case class ESConfig(val httpHosts:String, val trasportHosts:String, val index:String, val clustername:String)


/**
  *
  * @param rid    recomend   mid
  * @param score  recomend score
  */
case class Recommendation(rid:Int, score:Double)

/**
  *
  *
  * @param genres
  * @param recs
  */
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])


object StatisticsRecommender {


  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  val ES_MOVIE_INDEX = "Movie"
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )


    val sparkConf = new SparkConf().setAppName("statisticRecoomender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig = new MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._


    // data load
    val ratingDF = spark.read.option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .option("spark.mongodb.input.partitioner","MongoPaginateByCountPartitioner")  // with partition parameter
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read.option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .option("spark.mongodb.input.partitioner","MongoPaginateByCountPartitioner")
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // 统计 all the ratings of movies, group by mid
    ratingDF.createOrReplaceTempView("ratings")
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as cnt from ratings group by mid")
    rateMoreMoviesDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // every movie's ratings  every month
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // ms => us
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    val ratingOfYearMonth = spark.sql("select mid, score,changeDate(timestamp) as yearmonth from ratings")

    // register as a new table
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth,mid")

    rateMoreRecentlyMovies.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // avg ratings of every one movie

    val averageMovieDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")

    averageMovieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // top10 of each kind genre
    val movieScore = movieDF.join(averageMovieDF,Seq("mid","mid"))
    //所有的电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")
    val genresRDD = spark.sparkContext.makeRDD(genres)



    val genreTopMovies = genresRDD.cartesian(movieScore.rdd)
      .filter{
        case (genres,row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }.map{
      case (genres,row) => {
        (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
      }
    }.groupByKey()
      .map{
        case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }.toDF()

    genreTopMovies.write
      .option("uri", mongoConfig.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()



//    val genresTopMoviesDF = spark.sql("")

    spark.stop()
  }
}
