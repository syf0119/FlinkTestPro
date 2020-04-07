package day03

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object ListStateTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[(String, Int)] = environment.fromCollection(List(
      ("java", 1),
      ("python", 3),
      ("java", 2),
      ("scala", 2),
      ("python", 1),
      ("java", 1),
      ("scala", 2)
    ))
    data.keyBy(0).map(new ListStateTestFunctionMap).print()
    environment.execute()
  }

}
class ListStateTestFunctionMap extends RichMapFunction[(String,Int),(String,Int)]{

  var listState:ListState[(String,Int)]=_

  override def open(parameters: Configuration): Unit = {
    val ls = new ListStateDescriptor[(String,Int)]("ls",TypeInformation.of(new TypeHint[(String,Int)] {}))
    listState=getRuntimeContext.getListState(ls)
  }
  override def map(value: (String, Int)): (String, Int) = {
   val iter: util.Iterator[(String, Int)] = listState.get().iterator()
    if(iter.hasNext) {
      val tuple: (String, Int) = iter.next()
      listState.clear()
      listState.add((value._1,(value._2+tuple._2)))
    }
    else{
      listState.add(value)
    }
    listState.get().iterator().next()
  }
}
