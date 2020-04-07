package day01.like3

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object RebalanceTest {

  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    val source: DataSet[Long] = environment.generateSequence(0,1000)
    val filterData: DataSet[Long] = source.filter(_>500)
    filterData.rebalance()
    filterData.map(new RichMapFunction[Long,(Int,Long)]{
      var task=0

      override def open(parameters: Configuration): Unit = {
        task=getRuntimeContext.getIndexOfThisSubtask
      }

      override def map(value: Long): (Int, Long) = {
        task->value
      }
    }
    ).print()
  }

}
