package day02

import java.sql.{Connection, DriverManager, PreparedStatement}

import day02.MysqlTest.Demo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

object SinkMysqlTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[Demo] = environment.fromElements(Demo(3231231,"aaaaaaa"))
    data.addSink(new MysqlSink)
    environment.execute()
  }

  class MysqlSink extends RichSinkFunction[Demo]{
    var conn:Connection=null
    var psm:PreparedStatement=null
    override def open(parameters: Configuration): Unit = {
      val driver="com.mysql.jdbc.Driver"
      val url="jdbc:mysql://autumn:3306/test"
      val username="root"
      val password="0119"
      Class.forName(driver)
      conn=DriverManager.getConnection(url,username,password)
      psm=conn.prepareStatement("insert into demo values(?,?)")
    }

    override def invoke(value: Demo): Unit = {
      psm.setInt(1,value.id)
      psm.setObject(2,value.name)
      psm.executeUpdate()
      println("执行")

    }

    override def close(): Unit = {
      if(psm!=null) psm.close()
      if(conn!=null) conn.close()
    }

  }

}
