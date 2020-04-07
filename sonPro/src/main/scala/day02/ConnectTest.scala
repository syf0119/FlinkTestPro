package day02

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object ConnectTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[(Int, String)] = env.fromElements(1->"hello")
    val source2: DataStream[String] = env.fromElements("1","2","3","3","5")

    val conData: DataStream[String] = source.connect(source2).map(new CoMapFunction[(Int, String), String, String] {
      override def map2(value: String): String = {
        value + "s1"
      }

      override def map1(value: (Int, String)): String = {
        value + "s2"
      }
    })
    conData.print()
    env.execute()

  }

}
