package day02

import org.apache.flink.streaming.api.scala._

object CarWindow {


  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = environment.socketTextStream("spring",9999)
    val carData: DataStream[Car] = data.map { x =>
      println("map")
      Car(x.split(",")(0).toInt, x.split(",")(1).toInt)
    }
    carData.keyBy(0).countWindow(3,2).sum(
      1).print()
    environment.execute()
  }

}
case class Car(sensorId:Int,count:Int)
