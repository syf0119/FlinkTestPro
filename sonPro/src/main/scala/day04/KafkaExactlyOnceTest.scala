package day04

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaExactlyOnceTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.enableCheckpointing(3000)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setCheckpointTimeout(5000)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    environment.setStateBackend(new FsStateBackend("hdfs://spring:8020/check"))
    environment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val socketSource: DataStream[String] = environment.socketTextStream("spring",9999)
    val mapData: DataStream[String] = socketSource.map(x=>x)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","spring:9092,summer:9092,autumn:9092")
   // properties.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    val kafkaSink: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String]("test-1115",new SimpleStringSchema(),properties)
    mapData.addSink(kafkaSink)
    environment.execute()

  }
}
