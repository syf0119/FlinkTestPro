package day01.like

import org.apache.flink.api.scala._

object DataSet {
  /**
    * 1. 获取 ExecutionEnvironment 运行环境
    * 2. 使用 fromCollection 构建数据源
    * 3. 创建一个 User 样例类
    * 4. 使用 map 操作执行转换
    * 5. 打印测试
    */
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(4)
    val source: DataSet[String] = environment.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))
    val stus: DataSet[Student] = source.map(x => {
      Student(x.split(",")(0).toInt, x.split(",")(1))
    })
    stus.print()

  }
  case class Student(id:Int,name:String)

}

