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
    operation.insertOrUpdateAvailabilityRecord("anindex","atype","anid","D","E","F",client)
  }

}
