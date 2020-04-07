package day03

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadcastStateTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "spring:9092,summer:9092,autumn:9092")
    properties.setProperty("group.id", "test1115")
    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("test-1115", new SimpleStringSchema(), properties)
    val kafkadata: DataStream[String] = environment.addSource(kafkaConsumer)
    val mysqlData: DataStream[mutable.Map[String, (String, Int)]] = environment.addSource(new MysqlSourceFunction)
    val mapKafkaData: DataStream[(String, String, String, Int)] = kafkadata.map(line => {

      val json: JSONObject = JSON.parseObject(line)
      val userID: String = json.getString("userID")
      val eventTime: String = json.getString("eventTime")
      val eventType: String = json.getString("eventType")
      val productID: Int = json.getIntValue("productID")
      (userID, eventTime, eventType, productID)
    })
    val msState: MapStateDescriptor[Void, mutable.Map[String, (String, Int)]] = new MapStateDescriptor[Void, mutable.Map[String, (String, Int)]]("ms", classOf[Void], classOf[mutable.Map[String, (String, Int)]])
    val mysqlBroadcast: BroadcastStream[mutable.Map[String, (String, Int)]] = mysqlData.broadcast(msState)
    val connectData: BroadcastConnectedStream[(String, String, String, Int), mutable.Map[String, (String, Int)]] = mapKafkaData.connect(mysqlBroadcast)
    val result: DataStream[(String, String, String, Int, String, Int)] = connectData.process(new MyBroadcastFunction)
    result.print()
    environment.execute()

    


  }

}
class MyBroadcastFunction extends  BroadcastProcessFunction[(String, String, String, Int),
  mutable.Map[String, (String, Int)], (String, String, String, Int, String, Int)] {
  private val ms: MapStateDescriptor[Void, mutable.Map[String, (String, Int)]] = new MapStateDescriptor[Void, mutable.Map[String, (String, Int)]]("ms", classOf[Void], classOf[mutable.Map[String, (String, Int)]])


  override def processBroadcastElement(value: mutable.Map[String, (String, Int)], ctx: BroadcastProcessFunction[(String, String, String, Int), mutable.Map[String, (String, Int)], (String, String, String, Int, String, Int)]#Context, out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    val bs: BroadcastState[Void, mutable.Map[String, (String, Int)]] = ctx.getBroadcastState(ms)
    bs.put(null, value)
  }
  //查询广播流数据，进行数据合并，此方法，广播流只读
  override def processElement(value: (String, String, String, Int), ctx: BroadcastProcessFunction[(String, String, String, Int), mutable.Map[String, (String, Int)], (String, String, String, Int, String, Int)]#ReadOnlyContext, out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    val broadcastData: ReadOnlyBroadcastState[Void, mutable.Map[String, (String, Int)]] = ctx.getBroadcastState(ms)
    val map: mutable.Map[String, (String, Int)] = broadcastData.get(null)
    val tuple: (String, Int) = map.getOrElse(value._1,null)
    if(tuple!=null){
      out.collect(value._1,value._2,value._3,value._4,tuple._1,tuple._2)
    }


    }


  
}









class MysqlSourceFunction extends RichSourceFunction[mutable.Map[String, (String, Int)]] {

  var conn: Connection = null
  var psm: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://autumn:3306/test"
    val username = "root"
    val password = "0119"
    Class.forName(driver)
    conn = DriverManager.getConnection(url, username, password)
    psm = conn.prepareStatement("select * from user_info")
  }

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, (String, Int)]]): Unit = {
    val map = mutable.Map[String, (String, Int)]()
    val resultSet: ResultSet = psm.executeQuery()
    while (resultSet.next()) {
      val id: String = resultSet.getString(1)
      val name = resultSet.getString(2)
      val age = resultSet.getInt(3)
      map.put(id, name -> age)

    }
    ctx.collect(map)

  }

  override def close(): Unit = {
    if (psm != null) psm.close()
    if (conn != null) conn.close()
  }

  override def cancel(): Unit = ???
}


