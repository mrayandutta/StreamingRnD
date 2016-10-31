package alerting
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


/**
  * Created by Ayan on 10/31/2016.
  */
object SparkToESPersister
{
  def main(args: Array[String])
  {
    val conf = new SparkConf().setAppName("SparkToESPersister").setMaster("local[*]").
    set("es.nodes", "localhost").set("es.port", "9200")
    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("Kolkata" -> "Beliaghata", "Skill" -> "Spark")

    sc.makeRDD(Seq(numbers,airports)).saveToEs("aes/docs")
    sc.stop()
    println("Sprk Context Stopped")
  }
}
