package day01.like2

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.table.plan.logical.Aggregate

import scala.collection.mutable

object MinByTest {
  def main(args: Array[String]): Unit = {
    val data = new mutable.MutableList[(Int, String, Double)]
    data.+=((1, "yuwen", 90.0))
    data.+=((2, "shuxue", 20.0))
    data.+=((3, "yingyu", 30.0))
    data.+=((4, "wuli", 40.0))
    data.+=((5, "yuwen", 50.0))
    data.+=((6, "wuli", 60.0))
    data.+=((7, "yuwen", 70.0))
    //使用minBy操作，求集合中的元组数据，每个科目分数的最小值

    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val dataSet: DataSet[(Int, String, Double)] = environment.fromCollection(data)
      dataSet.groupBy(1).maxBy(2).print()
   // dataSet.groupBy(1).aggregate(Aggregations.MAX,2).print()
  }

}
