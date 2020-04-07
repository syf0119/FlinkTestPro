package day01.like2

import org.apache.flink.api.scala._

object MapPartition {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(String, Int)] = environment.fromElements(("python", 1), ("scala", 1), ("java", 1), ("java", 1), ("scala", 1), ("java", 1))
    data.mapPartition(line=>{
      line.map(x=>(x._1,x._2+1))
    }).print()
  }
}
