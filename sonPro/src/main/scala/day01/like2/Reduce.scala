package day01.like2

import org.apache.flink.api.scala._

object Reduce {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val source = env.fromElements(("java", 1), ("scala", 1), ("java", 1))
    val groupData: GroupedDataSet[(String, Int)] = source.groupBy(0)
    val reduceData: DataSet[(String, Int)] = source.reduce((x,y)=>x._1->(x._2+y._2))
    reduceData.print()


  }
}
