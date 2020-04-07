package day04



import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

import scala.util.Random

object StreamSqlTest {
  /**
    * 1)获取流处理运行环境
    * 2)获取Table运行环境
    * 3)设置处理时间为 EventTime
    * 4)创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
    * 5)创建一个自定义数据源
    * a.使用for循环生成1000个订单
    * b.随机生成订单ID（UUID）
    * c.随机生成用户ID（0-2）
    * d.随机生成订单金额（0-100）
    * e.时间戳为当前系统时间
    * f.每隔1秒生成一个订单
    * 6)添加水印，允许延迟2秒
    * 7)导入 import org.apache.flink.table.api.scala._ 隐式参数
    * 8)使用 registerDataStream 注册表，并分别指定字段，还要指定rowtime字段
    * 9)编写SQL语句统计用户订单总数、最大金额、最小金额
    * 分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
    * 10)使用 tableEnv.sqlQuery 执行sql语句
    * 11)将SQL的执行结果转换成DataStream再打印出来
    * 12)启动流处理程序
    *
    */
  def main(args: Array[String]): Unit = {
   val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: StreamTableEnvironment = TableEnvironment.getTableEnvironment(environment)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val data: DataStream[Order2] = environment.addSource(new StreamSqlTestSource)
    val waterData: DataStream[Order2] = data.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order2](Time.seconds(2)) {
      override def extractTimestamp(element: Order2): Long = {
        element.orderDate
      }
    })
    import org.apache.flink.table.api.scala._
    tableEnvironment.registerDataStream("tbl",waterData,'orderId,'userId,'orderPrice,'orderDate.rowtime)
    val sql = "select userId,sum(userId),max(orderPrice),min(orderPrice) from tbl group by userId,tumble(orderDate, interval '5' second)"
    val resultTable: Table = tableEnvironment.sqlQuery(sql)
    val result: DataStream[(Boolean, Row)] = tableEnvironment.toRetractStream[Row](resultTable)
    result.print()
    environment.execute()


  }


}
case class Order2(orderId:String,userId:Int,orderPrice:Double,orderDate:Long)

class StreamSqlTestSource extends RichSourceFunction[Order2]{
  override def cancel(): Unit = ???

  override def run(ctx: SourceFunction.SourceContext[Order2]): Unit = {
    for(x<- 0 to 1000){
      ctx.collect(Order2(UUID.randomUUID().toString,Random.nextInt(2),Random.nextInt(100),System.currentTimeMillis()))

      Thread.sleep(1000)
    }
  }
}

