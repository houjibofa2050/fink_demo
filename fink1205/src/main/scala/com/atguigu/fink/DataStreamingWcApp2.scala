package com.atguigu.fink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object DataStreamingWcApp2 {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = environment.socketTextStream("127.0.0.1",9000)
    val ds: DataStream[(String, Int)] = dataStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    ds.print()
    environment.execute()
}

}
