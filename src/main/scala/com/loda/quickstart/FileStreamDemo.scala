package com.loda.quickstart
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * @Author loda
 * @Date 2023/2/25 16:54
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object FileStreamDemo {
	def main(args: Array[String]): Unit = {
		// ==================== 建立连接 ====================
		val conf = new SparkConf().setMaster("local[*]").setAppName("FileStreamDemo")
		// 使用 Duration 对象构建批处理的间隔时间，单位毫秒
		//val streaming = new StreamingContext(conf, Duration(2 * 1000))
		// 使用 Milliseconds、Seconds、Minutes 对象构建批处理的间隔时间
		val ssc = new StreamingContext(conf, Seconds(2))
		// 日志级别
		ssc.sparkContext.setLogLevel("ERROR")
		// ==================== 业务处理 ====================
		// 监听 data/wordcount 目录
		val lines: InputDStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text,
			TextInputFormat](directory = "data/wordcount")
		// 处理 data/wordcount 目录下每个`新建文件`的数据
		val result: DStream[(String, Int)] = lines.flatMap(
			_._2.toString.split("\\s+")).map(_ -> 1)
			.reduceByKey(_ + _)
		result.print()
		// ==================== 启动流式计算 ====================
		// 启动流式计算
		ssc.start()
		// 调用 awaitTermination 防止应用退出
		ssc.awaitTermination()
	}
}