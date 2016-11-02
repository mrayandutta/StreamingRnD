package alerting
import org.apache.spark.{SparkConf, SparkContext}
//import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import collection.mutable.HashMap
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress


// define a case class
case class AvailabilityRecord(instanceName: String, status: String)
/**
  * Created by Ayan on 10/31/2016.
  */
object SparkToESPersister
{
  val transportClient = new TransportClient();
  transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

  def main(args: Array[String])
  {
    val conf = new SparkConf().setAppName("SparkToESPersister").setMaster("local[*]").
    set("es.nodes", "localhost").set("es.port", "9200")
    val sc = new SparkContext(conf)


    //val data: HashMap[String, Object] = HashMap("title"->"t1","content"-> "c1","postDate"-> "p1")
    val record1 = AvailabilityRecord("M1","ON");
    val record2 = AvailabilityRecord("M1","OFF");
    val record3 = AvailabilityRecord("M2","ON");
    val record4 = AvailabilityRecord("M2","OFF");
    //val rdd = sc.makeRDD(Seq(record1,record2,record3,record4))
    val indexAndTypeStr = "metric/availability"
    insertRecord("metric","availability","1","M1","ON")
    insertRecord("metric","availability","2","M2","ON")

    //rdd.saveToEs("metric/availability")
    //EsSpark.saveToEs(rdd,"metric/availability")
    //EsSpark.saveJsonToEs(rdd,"metric/availability")
    sc.stop()
    println("Spark Context Stopped")
  }

  def insertRecord(index: String,typeStr: String,id: String,instanceName: String, status: String) =
  {
    val data: HashMap[String, Object] = HashMap(instanceName->status)
    transportClient.prepareIndex(index, typeStr, id).setSource(data,"").execute().actionGet()
  }

  def getRecord(index: String,typeStr: String ,id: String) =
  {
    val response: GetResponse = transportClient.prepareGet(index, typeStr, id).execute().actionGet()
    print("response :"+response.getSource())

  }
}
