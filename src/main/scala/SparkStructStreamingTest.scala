import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime


object SparkStructStreamingTest {

  case class Weblog(datetime:String,
                    userid:String,
                    searchname:String,
                    retorder:String,
                    cliorder:String,
                    cliurl:String)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder().master("local[2]")
      .appName("streaming")
      .getOrCreate()


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
      .option("subscribe", "weblogs")
      .load()
    import spark.implicits._

    //df.show()
    val lines=df.selectExpr("CAST(value AS STRING)").as[String]


    // Split the lines into words
    val words = lines.map(_.split(",")).map(x => Weblog(x(0),x(1), x(2),x(3),x(4),x(5)))

    // Generate running word count
    val wordCounts = words.groupBy("searchname").
                        count().toDF("titleName","count")

    val url="jdbc:mysql://master:3306/test"
    val username="root"
    val password="123456"
    val writer=new JDBCSink(url,username,password)

//    val query = wordCounts.writeStream.foreach(writer)
//      .outputMode("update").
//      trigger(ProcessingTime("5 seconds"))
//      .start()
        val query = wordCounts.writeStream
          .outputMode("update").format("console").start()

    query.awaitTermination()
  }
}
