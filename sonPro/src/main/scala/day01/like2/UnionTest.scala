package day01.like2

import org.apache.flink.api.scala._

object UnionTest {
  def main(args: Array[String]): Unit = {
    //1.获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2.创建数据源
    val elements1 = env.fromElements("java")
    val elements2 = env.fromElements("scala")
    val elements3 = env.fromElements("java")

    //3.使用union联合操作
     elements1.union(elements2).union(elements3).distinct.print()

  }

}
