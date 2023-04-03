package com.loda.structed

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StructuredStreamingDemo {
	def main(args: Array[String]): Unit = {
		// ==================== 建立连接 ====================
		val conf = new SparkConf().setMaster("local[*]").setAppName("StructuredStreamingDemo")
		val spark = SparkSession.builder().config(conf).getOrCreate()
		// 日志级别
		spark.sparkContext.setLogLevel("ERROR")
		import spark.implicits._
		// ==================== 业务处理 ====================
		// 接收数据
		val lines = spark.readStream
			.format("socket")
			.option("host", "localhost")
			.option("port", 9999)
			.load()
		// 处理数据
		val words = lines.as[String].flatMap(_.split("\\s+"))
		val wordCounts = words.groupBy("value").count()
		// 输出数据
		val query = wordCounts.writeStream
			.outputMode("complete")
			.format("console")
			.start()
		// 调用 awaitTermination 防止应用退出
		query.awaitTermination()
	}
}