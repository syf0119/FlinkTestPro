package day02

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1 .+=((1, 1L, "Hi"))
    data1 .+=((2, 2L, "Hello"))
    data1 .+=((3, 2L, "Hello world"))
    val ds1 = env.fromCollection(data1)
    val data2 = new mutable.MutableList[(Int, Long, Int, String, Long)]
    data2 .+=((1, 1L, 0, "Hallo", 1L))
    data2 .+=((2, 2L, 1, "Hallo Welt", 2L))
    data2 .+=((3, 3L, 2, "Hallo Welt wie", 1L))
    val ds2 = env.fromCollection(data2 )
    val result: DataSet[(Int, Long, String, String)] = ds1.map(new RichMapFunction[(Int, Long, String), (Int, Long, String, String)] {
      var ds: util.List[(Int, Long, Int, String, Long)] = null

      override def open(parameters: Configuration): Unit = {
        ds = getRuntimeContext.getBroadcastVariable[(Int, Long, Int, String, Long)]("ds2")
      }

      import collection.JavaConverters._

      override def map(value: (Int, Long, String)): (Int, Long, String, String) = {
        var tuple: (Int, Long, String, String) = null
        for (line <- ds.asScala) {
          if (value._1 == line._2) {
            tuple = (value._1, value._2, value._3, line._4)
          }
        }
        tuple
      }
    }).withBroadcastSet(ds2, "ds2")
    result.print()


  }


}
