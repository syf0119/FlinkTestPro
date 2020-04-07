package day04

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._

object RestartStrategiesTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.enableCheckpointing(3000)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setCheckpointTimeout(5000)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //environment.setRestartStrategy(RestartStrategies.noRestart())
     //environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(5)))
    environment.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.seconds(30),Time.seconds(3)))
    val data: DataStream[String] = environment.socketTextStream("spring",9999)
    data.map(line=>{
      if("f".equals(line)) throw new RuntimeException("死机了")
      line
    }).print()
    environment.execute()

   }

}
