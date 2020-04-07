package day01.like

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
object WordCount {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(2)
    val source = environment.fromElements("aaa aaa aa a ww aaa")
    val value: GroupedDataSet[(String, Int)] = source.flatMap(_.split("\\W+"))
      .map(_ -> 1)
      .groupBy(_._1.length)

    val result: DataSet[(String, Int)] = value.reduce((x,y)=>x._1->(y._2+x._2))

    result.print()
   result.writeAsText("F://output.txt",WriteMode.OVERWRITE)
    environment.execute()







  }

}
