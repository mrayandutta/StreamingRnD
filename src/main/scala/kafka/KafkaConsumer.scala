package kafka

import java.util.Properties
import kafka.consumer.{ConsumerIterator, Consumer, ConsumerConfig, KafkaStream}
import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties
;


case class BasicConsumer(zooKeeper: String, groupId: String, waitTime: String)
{
  val kafkaProps = new Properties()
  var streams:Map[String, KafkaStream[String,String]] = Map.empty

  kafkaProps.put("zookeeper.connect", zooKeeper)
  kafkaProps.put("group.id",groupId)
  kafkaProps.put("auto.commit.interval.ms","1000")
  kafkaProps.put("auto.offset.reset","smallest");
  kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
  kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

  // un-comment this if you want to commit offsets manually
  //kafkaProps.put("auto.commit.enable","false");

  // comment this out if you want to wait for data indefinitely
  //kafkaProps.put("consumer.timeout.ms",waitTime)

  private val consumer = Consumer.create(new ConsumerConfig(kafkaProps))

  def subscribe(topic: String): Unit = {
    /* We tell Kafka how many threads will read each topic. We have one topic and one thread */
    val topicCountMap = Map[String, Int](topic -> 1)
    /* We will use a decoder to get Kafka to convert messages to Strings
        * valid property will be deserializer.encoding with the charset to use.
        * default is UTF8 which works for us */
    val decoder = new StringDecoder(new VerifiableProperties())
    val stream: KafkaStream[String, String] = consumer.createMessageStreams(topicCountMap, decoder, decoder).get(topic).get(0)
    streams += (topic -> stream)
  }

  def read(topic:String): String =  {
    val stream = streams.get(topic).get
    val it: ConsumerIterator[String, String] = stream.iterator()
    it.next().message()
  }

  def shutdown(): Unit = consumer.shutdown()



}

object KafkaConsumer
{
  def main(args: Array[String]): Unit = {

    val zooKeeper = "localhost:2181"
    val groupId = "1"
    val topic = "event"
    val waitTime = "1"

    val myConsumer = BasicConsumer(zooKeeper, groupId, waitTime)
    myConsumer.subscribe(topic)


    while(true)
    {
      println("working")
      val message = myConsumer.read(topic)
      if(message.length < 1)
      {
        myConsumer.shutdown()
        println("bye")
      }

      println(message)
    }
  }

}
