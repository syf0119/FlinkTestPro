package day03

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object MapStateTest {
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
    data.keyBy(0).map(new MapStateTestFunctionMap).print()
    environment.execute()
  }

}

class MapStateTestFunctionMap extends RichMapFunction[(String,Int),(String,Int)]{
  var mapState:MapState[String,Int]=_

  override def open(parameters: Configuration): Unit = {
    val ms = new MapStateDescriptor[String,Int]("ms",TypeInformation.of(new TypeHint[String] {}),TypeInformation.of(new TypeHint[Int] {}))
    mapState = getRuntimeContext.getMapState(ms)

  }
  override def map(value: (String, Int)): (String, Int) = {
    if(mapState.contains(value._1)){
      mapState.put(value._1,mapState.get(value._1)+value._2)
      println(mapState)
    }
    else{
      mapState.put(value._1,value._2)
    }

    value._1->mapState.get(value._1)

  }
}
