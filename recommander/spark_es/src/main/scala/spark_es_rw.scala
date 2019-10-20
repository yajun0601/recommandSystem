
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Tag data set
  * 15,
  * 339,
  * sandra 'boring' bullock, tag
  * 1138537770                timestamp
  * */
case class Tag(val uid:Int, val mid:Int, val tag:String, val timestamp:Int)

case class Person(id: Int, name: String, address: String)


object spark_es_rw {
  val TAG_DATA_PATH = "/Volumes/data/recommandSystem/recommander/dataloader/src/main/resources/tags.csv"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("es_rw").setMaster("local[*]")

    val spark = SparkSession.builder()
        .config(sparkConf)
      .config("es.nodes","localhost:9200")
      .config("pushdown","true")
      .config("es.index.auto.create","true")
      .config("es.nodes.wan.only","true")
      .getOrCreate()
    //       .enableHiveSupport()

    import spark.implicits._


    val df = spark.createDataFrame(List(Person(1, "xiaoming","beijing")))
    val map = Map("es.mapping.id" -> "id")
    EsSparkSQL.saveToEs(df, "myindex/mytype", map)



 /*   val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

    // tagRDD -> Dataframe
    val tagDF = tagRDD.map(item =>{
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

    println(tagDF.head().toString())

    try{
      EsSparkSQL.saveToEs(tagDF,"index/type")
    }catch {
      case es: Exception =>{
        print(es)
      }
    }*/




  }

}
