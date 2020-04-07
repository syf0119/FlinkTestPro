package day03

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
object KeyedStateTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    val data: DataStream[(Long, Long)] = environment.fromCollection(List(
      (1L, 4L),
      (2L, 3L),
      (3L, 1L),
      (1L, 2L),
      (3L, 2L),
      (1L, 2L),
      (2L, 2L),
      (2L, 9L))
    )
    val keyByData: KeyedStream[(Long, Long), Tuple] = data.keyBy(0)
    keyByData.map(new KeyedStateTestMap).print()
    environment.execute()

  }

}
class KeyedStateTestMap extends RichMapFunction[(Long, Long),(Long, Long)]{
 private var valueState:ValueState[(Long,Long)]=_
  override def open(parameters: Configuration): Unit = {
    val vs=new  ValueStateDescriptor[(Long,Long)]("vs",TypeInformation.of(new TypeHint[(Long,Long)]{}))
    valueState=getRuntimeContext.getState(vs)

  }
  override def map(value: (Long, Long)): (Long, Long) = {
    val tuple: (Long, Long) = valueState.value()
    if(tuple==null) {
      valueState.update(value)
      value
    }
   else{
      valueState.update(value._1->(value._2+tuple._2))
      value._1->(value._2+tuple._2)


    }


  }
}
