package day01.like3

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode


import scala.collection.mutable

object PartitionByHashTest {
  def main(args: Array[String]): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "Hello world, how are you?"))
    data.+=((5, 3L, "I am fine."))
    data.+=((6, 3L, "Luke Skywalker"))
    data.+=((7, 4L, "Comment#1"))
    data.+=((8, 4L, "Comment#2"))
    data.+=((9, 4L, "Comment#3"))
    data.+=((10, 4L, "Comment#4"))
    data.+=((11, 5L, "Comment#5"))
    data.+=((12, 5L, "Comment#6"))
    data.+=((13, 5L, "Comment#7"))
    data.+=((14, 5L, "Comment#8"))
    data.+=((15, 5L, "Comment#9"))
    data.+=((16, 6L, "Comment#10"))
    data.+=((17, 6L, "Comment#11"))
    data.+=((18, 6L, "Comment#12"))
    data.+=((19, 6L, "Comment#13"))
    data.+=((20, 6L, "Comment#14"))
    data.+=((21, 6L, "Comment#15"))

    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[(Int, Long, String)] = environment.fromCollection(data)
    val mapData: DataSet[(Int, Long, String)] = dataSet.partitionByHash(2).setParallelism(2).mapPartition {
      line => {

        line.map(x=>x)
      }
    }
    //mapData.writeAsText("F://maps",WriteMode.OVERWRITE)
    //environment.execute("hello")
    mapData.print()
  }

}
