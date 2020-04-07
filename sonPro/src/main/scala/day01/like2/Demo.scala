package day01.like2

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
object Demo {
  /**
  A;B;C;D;B;D;C
B;D;A;E;D;C
A;B
统计相邻字符串出现的次数(A+B , 2) (B+C , 1)....
    * */
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = environment.fromElements("A;B;C;D;B;D;C;B;D;A;E;D;C;A;B")
    val tuples: DataSet[immutable.IndexedSeq[(String, Int)]] = data.map(_.split(";")).map(line => {
      for (index <- 0 until (line.length - 1)) yield {
        line(index) + line(index + 1) -> 1
      }
    })

     val flatData: DataSet[(String, Int)] = tuples.flatMap(x=>x)
    val dataGroupBy: GroupedDataSet[(String, Int)] = flatData.groupBy(0)
    //dataGroupBy.sum(1).print()

    val sum: DataSet[(Int, Int)] = environment.fromElements((1,2),(3,12),(12,12),(12,11))
    sum.minBy(0).print()





  }
}

