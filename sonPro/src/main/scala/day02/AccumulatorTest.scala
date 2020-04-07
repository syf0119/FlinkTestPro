package day02

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    /**
      * 开发步骤：
      * 1.获取批处理执行环境
      * 2.加载本地数据集
      * 3.数据转换
      *   （1）定义累加器
      *   （2）注册累加器
      *   （3）使用累加器
      * 4.数据落地
      * 5.触发执行
      * 6.打印累加器结果
      */

    //1.获取批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2.加载本地数据集
    val source: DataSet[Int] = env.fromElements(1,2,3,4,5,6)

    //3.数据转换
    val result: DataSet[Int] = source.map(new RichMapFunction[Int, Int] {
      //（1）定义累加器
      private val counter = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        //（2）注册累加器
        getRuntimeContext.addAccumulator("counter", counter)
      }

      //（3）使用累加器
      override def map(value: Int): Int = {
        counter.add(value)
        value
      }
    })

    //4.数据落地
    result.writeAsText("counter")

    // 5.触发执行
    val res: JobExecutionResult = env.execute("counter")
    //6.打印累加器结果
    val i: Int = res.getAccumulatorResult[Int]("counter")
    println("累加器执行结果："+i)
  }

}
