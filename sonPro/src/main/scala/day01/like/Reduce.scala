package day01.like

import org.apache.flink.api.scala._
object Reduce {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[(String, Int)] = environment.fromElements(("python", 1), ("scala", 1), ("java", 1), ("java", 1), ("scala", 1), ("java", 1))
    val groupData: GroupedDataSet[(String, Int)] = source.groupBy(_._1)


    /* source.reduce((x,y)=>x._1->(y._2+x._2))
      .print()*/


  }
}