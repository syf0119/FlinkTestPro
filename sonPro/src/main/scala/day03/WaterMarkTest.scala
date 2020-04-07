package day03



import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val data: DataStream[String] = environment.socketTextStream("spring",9999)
    val bosses: DataStream[Boss] = data.map(x => {
      val fields: Array[String] = x.split(",")
      Boss(fields(0).toLong, fields(1), fields(2), fields(3).toDouble)
    })



    val waterData: DataStream[Boss] = bosses.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Boss] {
      val delayTime: Long = 2000L
      var currentTime: Long = 0L

      override def getCurrentWatermark: Watermark = {
        println("当前Watermarktime："+(currentTime-delayTime))
        new Watermark(currentTime - delayTime)
      }

      override def extractTimestamp(element: Boss, previousElementTimestamp: Long): Long = {
        currentTime = math.max(element.time, currentTime)
        println("当前最大时间currentTime："+currentTime)
        element.time
      }

    })
    val outputTag = new OutputTag[Boss]("outputTag")
    val result: DataStream[Boss] = waterData
      .keyBy(_.boss)
      .timeWindow(Time.seconds(3))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(outputTag)
      .maxBy(3)


    result.print("正常数据：")
    result.getSideOutput(outputTag).print("迟到数据")
    environment.execute("hello")
  }

}
case class Boss(time: Long, boss: String, product: String, price: Double)