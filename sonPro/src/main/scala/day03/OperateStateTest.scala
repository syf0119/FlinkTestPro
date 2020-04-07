package day03

import java.util
import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object OperateStateTest {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.enableCheckpointing(3000)
    environment.setStateBackend(new FsStateBackend("hdfs://spring:8020/check"))
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(5)))
    val data: DataStream[Long] = environment.addSource(new MySourceFunc)
    println(".....")
    data.print("消费count=")
    println()
    //Thread.sleep(5000)
    println()
    environment.execute()


  }

}
class MySourceFunc extends SourceFunction[Long] with CheckpointedFunction {
  var listState:ListState[Long]=_
  var count:Long=0


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    println("run 方法启动")
    val iter: util.Iterator[Long] = listState.get().iterator()
    while (iter.hasNext){
      count=iter.next()
      println("读取listState数据count："+count)
      Thread.sleep(5000)

    }
    while(true){
     count += 1
      ctx.collect(count)
      println("生产数据"+count)
      Thread.sleep(5000)
      if(count==10 ||count==11) 1/0

    }

  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println("snapshotState 执行   count="+count+"  把count从hdfs写入磁盘")
    println()
    listState.clear()
    listState.add(count)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

    println("initializeState启动 count="+count)
    println()
    //定义listState
    val ls = new ListStateDescriptor[Long]("ls", TypeInformation.of(new TypeHint[Long] {}))
    //通过上下文对象context获取
    listState = context.getOperatorStateStore.getListState(ls)
  }
  override def cancel(): Unit = ???
}
