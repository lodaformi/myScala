package com.loda.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo {
	def main(args: Array[String]): Unit = {
		// ==================== 建立连接 ====================
		val conf = new SparkConf().setMaster("local[*]").setAppName("WindowDemo")
		// 使用 Duration 对象构建批处理的间隔时间，单位毫秒
		//val streaming = new StreamingContext(conf, Duration(2 * 1000))
		// 使用 Milliseconds、Seconds、Minutes 对象构建批处理的间隔时间
		val ssc = new StreamingContext(conf, Seconds(2))
		// 日志级别
		ssc.sparkContext.setLogLevel("ERROR")
		// ==================== 业务处理 ====================
		val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
		val windowDS: DStream[String] = lines.window(Seconds(4), Seconds(2))
		val result: DStream[(String, Int)] = windowDS.flatMap(_.split("\\s+")).map(_ -> 1).reduceByKey(_ + _)
		result.print()
		// ==================== 启动流式计算 ====================
		// 启动流式计算
		ssc.start()
		// 调用 awaitTermination 防止应用退出
		ssc.awaitTermination()
	}
}