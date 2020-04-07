package day02

import org.apache.flink.streaming.api.scala._
object WordCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = environment.socketTextStream("spring",9999)
   // data.flatMap(_.split("\\+W")).map(_->1).keyBy(0).sum(1).print()
    data.print()
    environment.execute()

  }

}
