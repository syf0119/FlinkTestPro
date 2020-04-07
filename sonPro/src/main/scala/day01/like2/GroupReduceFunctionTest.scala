package day01.like2

import java.lang
import collection.JavaConverters._

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
object GroupReduceFunctionTest {

  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(String, Int)] = environment.fromElements(("java", 1), ("scala", 1), ("java", 1),("java", 1), ("scala", 1), ("java", 1) )
    val result: DataSet[(String, Int)] = data.reduceGroup(new MyRduceGroup )
    result.print()
  }
}
class MyRduceGroup extends GroupReduceFunction[(String, Int), (String, Int)]with GroupCombineFunction[(String, Int), (String, Int)]{
  override def reduce(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {

    println("执行reduce")
    for(e<-values.asScala){
      println(e._1)
    }
    out.collect("hello"->121)
    out.collect("world"->886)
  }

  override def combine(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
   println("执行combine")
    for(e<-values.asScala){
      println(e._1)
    }
  }
}