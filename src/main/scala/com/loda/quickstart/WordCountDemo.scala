package com.loda.quickstart

/**
 * @Author loda
 * @Date 2023/2/25 16:44
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object WordCountDemo {
	def main(args: Array[String]): Unit = {
		// ==================== 建立连接 ====================
		val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
		// 使用 Duration 对象构建批处理的间隔时间，单位毫秒
		// val streaming = new StreamingContext(conf, Duration(2 * 1000))
		// 使用 Milliseconds、Seconds、Minutes 对象构建批处理的间隔时间
		val ssc = new StreamingContext(conf, Seconds(2))
		// 日志级别
		ssc.sparkContext.setLogLevel("ERROR")
		// ==================== 业务处理 ====================
		// 监听 localhost:9999
		val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
		// 处理发送给 localhost:9999 端口的数据
		val result: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map(_ -> 1).reduceByKey(_ + _)
		// 打印数据，默认输出每个 RDD 的前 10 个元素
		result.print()
		// ==================== 启动流式计算 ====================
		// 启动流式计算
		ssc.start()
		// 调用 awaitTermination 防止应用退出
		ssc.awaitTermination()
	}
}
