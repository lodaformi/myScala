package com.loda.quickstart

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Random

object CustomStreamDemo {
	def main(args: Array[String]): Unit = {
		// ==================== 建立连接 ====================
		val conf = new SparkConf().setMaster("local[*]").setAppName("CustomStreamDemo")
		// 使用 Duration 对象构建批处理的间隔时间，单位毫秒
		//val streaming = new StreamingContext(conf, Duration(2 * 1000))
		// 使用 Milliseconds、Seconds、Minutes 对象构建批处理的间隔时间
		val ssc = new StreamingContext(conf, Seconds(2))
		// 日志级别
		ssc.sparkContext.setLogLevel("ERROR")
		// ==================== 业务处理 ====================
		// 通过自定义接收器接收的数据流创建 Dstream
		val custom: ReceiverInputDStream[String] = ssc.receiverStream(new CustomReceiver)
		custom.print()
		// ==================== 启动流式计算 ====================
		// 启动流式计算
		ssc.start()
		// 调用 awaitTermination 防止应用退出
		ssc.awaitTermination()
	}
	/**
	 * 自定义收集器
	 */
	class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
		private var flag = true;
		override def onStart(): Unit = {
			new Thread("Custom Socket Receiver") {
				override def run() {
					while (flag) {
						// 模拟数据产生时间
						Thread.sleep(500)
						// 模拟数据来源
						val i: String = new Random().nextInt(10).toString
						// store 方法存储数据
						store(i)
					}
				}
			}.start()
		}
		override def onStop(): Unit = {
			flag = false
		}
	}
}