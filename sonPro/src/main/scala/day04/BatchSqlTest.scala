package day04

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink


object BatchSqlTest {
  /**
    * 开发步骤：
    * 1)获取一个批处理运行环境
    * 2)获取一个Table运行环境
    * 3)创建一个样例类 Order 用来映射数据（订单名、用户名、订单日期、订单金额）
    * 4)基于本地 Order 集合创建一个DataSet source
    * 5)使用Table运行环境将DataSet注册为一张表
    * 6)使用SQL语句来操作数据（统计用户消费订单的总金额、最大金额、最小金额、订单总数）
    * 7)使用TableEnv.toDataSet将Table转换为DataSet
    * 8)打印测试
    */
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: BatchTableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val data: DataSet[Order] = environment.fromElements(
      Order(1, "zhangsan", "2018-10-20 15:30", 358.5),
      Order(2, "zhangsan", "2018-10-20 16:30", 131.5),
      Order(3, "lisi", "2018-10-20 16:30", 127.5),
      Order(4, "lisi", "2018-10-20 16:30", 328.5),
      Order(5, "lisi", "2018-10-20 16:30", 432.5),
      Order(6, "zhaoliu", "2018-10-20 22:30", 451.0),
      Order(7, "zhaoliu", "2018-10-20 22:30", 362.0),
      Order(8, "zhaoliu", "2018-10-20 22:30", 364.0),
      Order(9, "zhaoliu", "2018-10-20 22:30", 341.0))
    tableEnvironment.registerDataSet("ord",data)
    val sql = "select userName,sum(orderPrice),max(orderPrice),min(orderPrice),count(*) from ord group by userName"
    val resultTable: Table = tableEnvironment.sqlQuery(sql)
    resultTable.writeToSink(new CsvTableSink("csv",",",1,WriteMode.OVERWRITE))
    environment.execute()

  }
}
case class Order(orderId:Int,userName:String,orderDate:String,orderPrice:Double)