package day01.like4

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object ReadDfsTest {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = environment.readTextFile("hdfs://spring:8020/user.txt")
    data.print()
  }

}
