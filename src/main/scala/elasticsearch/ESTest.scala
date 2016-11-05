package elasticsearch

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

/**
  * Created by Ayan on 11/3/2016.
  */
object ESTest {

  def main(args: Array[String]): Unit = {
    val client = new TransportClient();
    client.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

    val operation: ESOperation = new ESOperation {}
    //operation.insertOrUpdateAvailabilityRecord("anindex","atype","anid","D","E","F",client)
    val esIndex ="events"
    val esType ="availability"
    val instance ="M1"
    val status ="ON"
    val time ="t1"
    val termFieldName ="instance"
    val termFieldValue="D"
    //operation.getAvailabilityRecordField(esIndex,esType,id,"status",client)
    operation.insertOrUpdateAvailabilityRecord(esIndex,esType,instance,status,time,client)
  }

}
