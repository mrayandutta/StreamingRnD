package kafka

import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import scala.util.Random
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date
/**
  * Created by Ayan on 10/30/2016.
  */
//object KafkaProducer extends App
object KafkaProducer
{

  def main(args: Array[String]): Unit =
  {
    val events = 20;
    val topic = "event";
    val brokers = "localhost:9092"

    val rnd = new Random()
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
    props.put("producer.type", "async")
    //props.put("request.required.acks", "1")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events))
    {
      val runtime = new Date().getTime();
      val instance = "Instance=M" + nEvents
      val status = "Status=ON"
      val time ="Time="+new Date().getTime();
      val msg = instance + "," + status + ","+time
      val ip = "192.168.2." + rnd.nextInt(255);
      val data = new KeyedMessage[String, String](topic, ip, msg);
      producer.send(data);
    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t));
    producer.close();
  }
}
