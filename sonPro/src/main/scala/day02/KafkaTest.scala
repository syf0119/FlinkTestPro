package day02

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","spring:9092,summer:9092,autumn:9092")
    properties.setProperty("group.id","test1115")
    properties.setProperty("auto.offset.reset", "latest")  //最近消费，是与便宜量相关
    properties.setProperty("flink.partition-discovery.interval-millis", "5000")
    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("test-1115",new SimpleStringSchema(),properties)
    val data: DataStream[String] = environment.addSource(kafkaConsumer)
    data.print()
    environment.execute()
  }

}
