package day01.like2

import org.apache.flink.api.scala._

import scala.collection.mutable

object JoinTest {
  def main(args: Array[String]): Unit = {
    val data1 = new mutable.MutableList[(Int, String, Double)]
    //学生学号---学科---分数
    data1.+=((1, "yuwen", 90.0))
    data1.+=((2, "shuxue", 20.0))
    data1.+=((3, "yingyu", 30.0))
    data1.+=((4, "yuwen", 40.0))
    data1.+=((5, "shuxue", 50.0))
    data1.+=((6, "yingyu", 60.0))
    data1.+=((7, "yuwen", 70.0))
    data1.+=((8, "yuwen", 20.0))
    //数据源-2
    val data2 = new mutable.MutableList[(Int, String)]
    //学号 ---班级
    data2.+=((1,"class_1"))
    data2.+=((2,"class_1"))
    data2.+=((3,"class_2"))
    data2.+=((4,"class_2"))
    data2.+=((5,"class_3"))
    data2.+=((6,"class_3"))
    data2.+=((7,"class_4"))
    data2.+=((8,"class_1"))
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet1: DataSet[(Int, String, Double)] = environment.fromCollection(data1)
    val dataSet2: DataSet[(Int, String)] = environment.fromCollection(data2)
    dataSet1.join(dataSet2).where(0).equalTo(0){
      (s1,s2)=>{
        (s1._1,s1._2,s1._3,s2._2)
      }
    }.print()

  }



}
