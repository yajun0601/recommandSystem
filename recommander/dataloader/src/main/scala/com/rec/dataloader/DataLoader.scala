package com.rec.dataloader


import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import java.net.InetAddress


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



// 数据主加载服务
object DataLoader {
  val MOVIE_DATA_PATH = "D:\\git\\Recommand\\recommander\\dataloader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\git\\Recommand\\recommander\\dataloader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\git\\Recommand\\recommander\\dataloader\\src\\main\\resources\\tags.csv"


  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {


    val config=Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://10.22.1.5:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" ->"192.168.10.32:9200",
      "es.transportHosts" ->"192.168.10.32:9300",
      "es.index"->"recommender",
      "es.cluster.name"->"wallet"
    )

    // spark config
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)
    // spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val ratingRDD =spark.sparkContext.textFile(RATING_DATA_PATH)
    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

    // movieRDD -> Dataframe, ETL done before this
    val movieDF = movieRDD.map(item =>{
      val attr = item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()
    // ratingRDD -> Dataframe
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    // tagRDD -> Dataframe
    val tagDF = tagRDD.map(item =>{
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)
    // store into mongodb
    storeDataInMongoDB(movieDF, ratingDF, tagDF)
    // store data into ES

    // 首先将 Tag 数据集处理， MID, tag1|tag2|tag3
    import org.apache.spark.sql.functions._
    /**
      * MID, Tags
      * 1   tag1|tag2|tag3
      */
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags")).select("mid","tags")
    // 新 tag  ==》 movies 数据集

    val movieWithTagsDF = movieDF.join(newTag,Seq("mid","mid"),"left")
    // 声明了一个ES配置的隐式参数
    implicit  val esConfig = ESConfig(config.get("es.httpHosts").get,config.get("es.transportHosts").get,config.get("es.index").get,config.get("es.cluster.name").get)
    println(esConfig.toString)
    // 新movie  =》 ES
    /**  Fix  the confilct
      * java.lang.IllegalStateException: availableProcessors is already set to [4], rejecting [4]
      * at io.netty.util.NettyRuntime$AvailableProcessorsHolder.setAvailableProcessors(NettyRuntime.java:51)
      */
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    // 新movie  =》 ES
    //storeDataInES(movieWithTagsDF)

    spark.stop()
  }

  def storeDataInMongoDB(movieDF:DataFrame, ratingDF:DataFrame, tagDF:DataFrame)(implicit mongoConfig: MongoConfig):Unit={
    // create connection
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // if mongo has db, delete them
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // write data into mongoDB  mongo spark connect
    movieDF.write.option("uri", mongoConfig.uri)
        .option("spark.mongodb.input.partitioner","MongoPaginateByCountPartitioner")
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write.option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write.option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // create index
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    // close connection
    mongoClient.close()
  }
  def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESConfig):Unit={
    // create setting
    val settings:Settings = Settings.builder().put("cluster.name",eSConfig.clustername).build()
    // create client
    val esClient = new PreBuiltTransportClient(settings)

/* old implementation
    val REGEX_HOST_PORT = "(.+):(\\D+)".r

    eSConfig.trasportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String, port:String) =>{
        print(host + ":" + port)
        esClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host),port.toInt)) // new api?
      }
    }
      "es.httpHosts" ->"192.168.10.32:9200",
      "es.transportHosts" ->"192.168.10.32:9300",
      "es.index"->"recommender",
      "es.cluster.name"->"elasticsearch"
    */

    // on startup// on startup  在这种情况下，只需要一个 host， 用简单粗暴的方式处理
    esClient.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.10.32"),9300))

    // clean old data
    if(esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))
    println("wrting data into ES" + eSConfig.toString)
    //write into ES
    movieDF.write
      .option("es.nodes",eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + '/' + ES_MOVIE_INDEX)
  }
}
