package day02


import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.scala._
object MysqlTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.addSource(new MysqlSource).print()
    environment.execute()

  }

 class MysqlSource extends RichSourceFunction[Demo] {
   var conn:Connection=null
   var psm:PreparedStatement=null
   override def open(parameters: Configuration): Unit = {
     val driver="com.mysql.jdbc.Driver"
     val url="jdbc:mysql://autumn:3306/test"
     val username="root"
     val password="0119"
     Class.forName(driver)
     conn=DriverManager.getConnection(url,username,password)
     psm=conn.prepareStatement("select * from demo")

   }
    override def cancel(): Unit = {
      if(psm!=null) psm.close()
      if(conn!=null) conn.close()
    }

    override def run(ctx: SourceFunction.SourceContext[Demo]): Unit = {
      val resultSet: ResultSet = psm.executeQuery()
      while (resultSet.next()){
        val id: Int = resultSet.getInt(1)
        val name: String = resultSet.getString(2)
        ctx.collect(Demo(id,name))
      }

    }
  }

  case class Demo(id:Int,name:String)

}
