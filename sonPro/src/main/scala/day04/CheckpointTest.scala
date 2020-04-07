package day04

import java.{lang, util}

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object CheckpointTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.enableCheckpointing(3000)
    //environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setCheckpointTimeout(5000)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //environment.setRestartStrategy(RestartStrategies.noRestart())
    //environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(5)))
    val data: DataStream[Info] = environment.addSource(new MySource )

    val keyedData: KeyedStream[Info, Long] = data.keyBy(_.id)
    keyedData.timeWindow(Time.seconds(4),Time.seconds(1))
        .apply(new CheckpointFunc).print()
    environment.execute()

  }

}
case class Info(id: Long, name: String, info: String, count: Int)
class MySource extends RichSourceFunction[Info]{
  override def cancel(): Unit = ???

  override def run(ctx: SourceFunction.SourceContext[Info]): Unit = {
    while(true){
      for(x<- 1 to 1000){
        ctx.collect(Info(1,"tom","hello world",x))
      }
      Thread.sleep(1000)
    }
  }
}
class MyState extends Serializable{
  var total:Long=0
  def getTotal=total
  def setTotal(total:Long)= this.total=total
}
class CheckpointFunc extends WindowFunction[Info,Long,Long,TimeWindow]
with ListCheckpointed[MyState]{
  var total:Long=0
  override def apply(key: Long, window: TimeWindow, input: Iterable[Info], out: Collector[Long]): Unit = {

    println("执行CheckpointFunc")

    input.foreach(x=>total+=x.count)
    out.collect(total)

  }

  override def restoreState(states: util.List[MyState]): Unit = {
    total = states.get(0).getTotal


  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[MyState] = {
    val states = new util.ArrayList[MyState]
    val myState = new MyState
    states.add(myState )
    states
  }
}
