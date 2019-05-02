import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {

  def main(args: Array[String]): Unit = {
    // SparkConf
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))

    // create spark session
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = new MongoConfig(config("mongo.uri"),config("mongo.db"))
    // read mongodb


    import sparkSession.implicits._

    // load rating data
    val ratingRDD = sparkSession.read
        .option("uri",mongoConfig.uri)
        .option("collection",OfflineRecommender.MONGODB_RATING_COLLECTION)
        .load()
        .as[MovieRating]
        .rdd
        .map(rating => Rating(rating.uid,rating.mid,rating.score)).cache()

    // output best parameter
    // 均方根误差 RMSE  least
    adjustALSParams(ratingRDD)

    sparkSession.close()
  }

  /**
    * output best parameters
    * @param trainData
    */
  def adjustALSParams(trainData:RDD[Rating]): Unit={
    val result = for(rank <- Array(30,40,50,60,70); lambda <- Array(1,0.1,0.01))
      yield {
        val model = ALS.train(trainData,rank,5,lambda)
        val rmse = getRmse(model,trainData)
        println("result:rank:"+rank + "==lambda: " + lambda +"==rmse: " + rmse)
        (rank,lambda,rmse)

      }

    println("ALSParams:" + result.sortBy(_._3).head)
  }

  /**
    *
    * @param model
    * @param trainData
    * @return
    */
  def getRmse(model:MatrixFactorizationModel, trainData:RDD[Rating]):Double={
    // construct a userProducts  RDD
    val userMovies = trainData.map(item => (item.user,item.product))
    val predictedRating = model.predict(userMovies)

    //compare predicted with real

    val real = trainData.map(item => ((item.user,item.product), item.rating))
    val predict = predictedRating.map(item => ((item.user,item.product),item.rating))

    sqrt(real.join(predict).map{
      case ((uid,mid),(real,pre))=>
        val err = real - pre
        err * err
    }.mean())
  }


}
