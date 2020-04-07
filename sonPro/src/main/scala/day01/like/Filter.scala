package day01.like

import org.apache.flink.api.scala._

object Filter {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[Int] = environment.fromElements(3,2,1,23,12,123,12313,123,1312312312)
    source.filter(_>10000).print()
  }
}
