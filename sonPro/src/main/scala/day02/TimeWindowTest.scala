package day02


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TimeWindowTest {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = environment.socketTextStream("spring",9999)
    val carData: DataStream[day02.Car] = data.map { x =>
      println("map")
      day02.Car(x.split(",")(0).toInt, x.split(",")(1).toInt)
    }
    carData.keyBy(0)
          .timeWindow(Time.seconds(6))
        //.timeWindow(Time.seconds(6),Time.seconds(3))

       .apply(new ApplyFunc).print()
    environment.execute()
  }

}
//case class Car(id:Int,count:Int)
class ApplyFunc extends WindowFunction[day02.Car,day02.Car,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[day02.Car], out: Collector[day02.Car]): Unit = {

    var key = 0
    var count = 0
    for(line <- input){

      key =1
      count = count + line.count
    }
    out.collect(day02.Car(key,count))
  }
}
