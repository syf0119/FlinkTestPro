package day01.like2

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
object ReduceGroup {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(String, Int)] = environment.fromElements(("java", 1), ("scala", 1), ("java", 1),("java", 1), ("scala", 1), ("java", 1) )
    val group: GroupedDataSet[(String, Int)] = data.groupBy(0)
    val result: DataSet[(String, Int)] = group.reduceGroup {
      (in: Iterator[(String, Int)], out: Collector[(String, Int)]) =>
        val tuple = in.reduce((x, y) => (x._1, x._2 + y._2))
        out.collect(tuple)

    }

    result.print()

  }

}
