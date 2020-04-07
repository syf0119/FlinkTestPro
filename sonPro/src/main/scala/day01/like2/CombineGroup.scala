package day01.like2

import java.lang

import org.apache.flink.api.common.functions.GroupCombineFunction
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object CombineGroup {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(String, Int)] = environment.fromElements(("java", 1), ("scala", 1), ("java", 1),("java", 1), ("scala", 1), ("hello", 1))
    val result : DataSet[(String, Int)] = data.combineGroup(new CombineGoupFunc)
    result.print()

  }
}
import collection.JavaConverters._
class CombineGoupFunc extends  GroupCombineFunction[(String, Int), (String, Int)] {
  override def combine(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit ={
    var name:String=""
    var num:Int=0;
    for(e<-values.asScala){
      name=e._1
      num+=e._2
    }
    out.collect(name->num)
  }

}
