package day01.like

import org.apache.flink.api.scala._

object MapPartition {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[(String, Int)] = environment.fromElements(("java", 1), ("scala", 1), ("java", 1),("java", 1), ("scala", 1), ("java", 1) 	)


      source.map(x=>(x._1,x._2))
   .print()  //4. 打印测试
  }

}
