package day02

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object SinkKafkaTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketSource: DataStream[String] = environment.socketTextStream("spring",9999)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","spring:9092,summer:9092,autumn:9092")
    val data: DataStream[String] = environment.fromElements("1111111111111111111")
    val pro: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String]("test-1115",new SimpleStringSchema(),properties)
    data.addSink(pro)

    socketSource.addSink(pro)
    socketSource.print()
    environment.execute()
  }

}
