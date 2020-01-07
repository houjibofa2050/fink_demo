package com.atguigu.fink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DataStreamingWcApp {
  //实时批处理wordcount
  def main(args: Array[String]): Unit = {
    print("hello world\n")
    //1.构建环境 environment 2.读取文件source   3.transfer  4 sink
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataStream[String] = env.socketTextStream("127.0.0.1", 9000)
    val dataSteam: DataStream[(String, Int)] = dataSet.flatMap((_: String).split(" ")).map((_, 1)).keyBy(0).sum(1)
    dataSteam.print()
    env.execute()


  }

}
