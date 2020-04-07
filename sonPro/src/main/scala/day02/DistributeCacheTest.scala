package day02

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object DistributeCacheTest {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val clazz: DataSet[Clazz] = environment.fromElements(
      Clazz(1, "class_1"),
      Clazz(2, "class_1"),
      Clazz(3, "class_2"),
      Clazz(4, "class_2"),
      Clazz(5, "class_3"),
      Clazz(6, "class_3"),
      Clazz(7, "class_4"),
      Clazz(8, "class_1")
    )
    environment.registerCachedFile("hdfs://spring:8020/subject.txt","cache")
    clazz.map(new RichMapFunction[Clazz,Info] {
      private val list=new ArrayBuffer[String]()
      override def open(parameters: Configuration): Unit = {
           val file: File = getRuntimeContext.getDistributedCache.getFile("cache")
          val strings: Iterator[String] = Source.fromFile(file).getLines()
           while (strings.hasNext){
             list.append(strings.next())
           }
      }
      override def map(value: Clazz): Info = {
        var outInfo:Info=null
        for(info<-list){
          if(info.split(",")(0).toInt==value.id){
            outInfo=Info(value.id,value.clazz,info.split(",")(1),info.split(",")(2).toDouble)
          }
        }
        if(outInfo==null) outInfo=Info(value.id,value.clazz,"null",0)
        outInfo
      }
    }).print()
  }

}
case class Clazz(id: Int, clazz: String)

//组成：(学号 ， 班级 ， 学科 ， 分数)
case class Info(id: Int, clazz: String, subject: String, score: Double)