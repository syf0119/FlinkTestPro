package day01.like2

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer
object LeftOuterJoinTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"zhangsan"))
    data1.append((2,"lisi"))
    data1.append((3,"wangwu"))
    data1.append((4,"zhaoliu"))

    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((4,"guangzhou"))


    val dataSet1: DataSet[(Int, String)] = env.fromCollection(data1)
    val dataSet2: DataSet[(Int, String)] = env.fromCollection(data2)
    dataSet1.leftOuterJoin(dataSet2).where(0).equalTo(0){
      (s1,s2)=>{
        if(s2==null) (s1._1,s1._2,"null")
        else (s1._1,s1._2,s2._2)
      }
    }.print()

  }

}
