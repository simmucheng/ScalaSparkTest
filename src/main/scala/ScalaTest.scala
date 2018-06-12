import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

object ScalaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("192.168.88.135", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
