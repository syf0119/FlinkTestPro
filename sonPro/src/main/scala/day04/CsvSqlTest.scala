package day04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object ReadCsv {

  def main(args: Array[String]): Unit = {

    //获取批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取表执行环境
    val tblEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    /**
      * * For example:
      * *
      * * {{{
      *     *   val source: CsvTableSource = new CsvTableSource.builder()
      *     *     .path("/path/to/your/file.csv")
      *     *     .field("myfield", Types.STRING)
      *     *     .field("myfield2", Types.INT)
      *     *     .build()
      *     * }}}
      */
    //获取tableSource
    val source: CsvTableSource = CsvTableSource.builder()
      .path("C:\\Users\\zhb09\\Desktop\\write\\test\\test.csv")
      .field("name", Types.STRING)
      .field("address", Types.STRING)
      .field("age", Types.INT)
      .ignoreFirstLine()
      .fieldDelimiter(",")
      .lineDelimiter("\n")
      .build()

    //注册tableSource成表
    tblEnv.registerTableSource("csv",source)

    //数据查询
    //(1)table api
    //val table: Table = tblEnv.scan("csv").select("age,name").where("age > 20")
    //(2)sql
    val table: Table = tblEnv.sqlQuery("select age,name from csv where age>20")

    //table转dataSet
    tblEnv.toDataSet[Row](table).print()

  }
}