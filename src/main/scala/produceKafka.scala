/*import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.scala.Logger

import scala.io.Source
import scala.util.Random

object produceKafka {
  def main(args: Array[String]): Unit = {
    val logger = Logger(this.getClass)
    val kafkaProp = new Properties()
    kafkaProp.put("bootstrap.servers", "localhost:9092")
    kafkaProp.put("acks", "all")
    kafkaProp.put("retries", "3")
    //  kafkaProp.put("batch.size", 16384)//16k
    //     kafkaProp.put("linger_ms", "2000")
    kafkaProp.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProp.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](kafkaProp)
    val lines = Source.fromFile("/Users/huan/Downloads/20200202/event-recommendation-engine-challenge/user_friends.csv").getLines()
    while (lines.hasNext) {
      val line = lines.next()
      val record = new ProducerRecord[String, String]("logs", line)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata != null) {
            logger.info("发送成功")
          }
          if (exception != null) {
            logger.info("消息发送失败")
          }
        }
      }
      )
      Thread.sleep(Random.nextInt(1000))
    }
  }
}
*/