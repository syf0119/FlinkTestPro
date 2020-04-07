package day02

import org.apache.flink.streaming.api.scala._

object SplitAndSelectTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[Int] = environment.fromElements(1,2,3,4,5,6)
    data.split(x=>{
      x%2 match {
        case 0 =>Traversable("even")
        case 1=>Traversable("even")
      }
    }).select("even").print()
    environment.execute()

  }

}
