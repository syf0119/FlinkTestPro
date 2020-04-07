package day01.like2

import org.apache.flink.api.scala._

import scala.collection.mutable


object DistinctTest {

  def main(args: Array[String]): Unit = {
    val data: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList
    data.+=((1, "yuwen", 89.0))
    data.+=((2, "shuxue", 92.2))
    data.+=((3, "yingyu", 89.99))
    data.+=((4, "wuli", 93.00))
    data.+=((5, "yuwen", 89.0))
    data.+=((6, "wuli", 93.00))
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[(Int, String, Double)] = environment.fromCollection(data)
    dataSet.distinct(1).print()


  }
}
