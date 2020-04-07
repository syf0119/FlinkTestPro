package day01.like4

import org.apache.flink.api.scala._

object ReadFromCsv {
  def main(args: Array[String]): Unit = {

      val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(String, String, Int)] = environment.readCsvFile[(String, String, Int)](
      "F:\\test.csv",
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2)
    )

    data.print()





    }






}
