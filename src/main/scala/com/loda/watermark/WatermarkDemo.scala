package com.loda.watermark

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

object WatermarkDemo {
	case class WordCount(word: String, timestamp: Timestamp)

	def main(args: Array[String]): Unit = {
		// ==================== 建立连接 ====================
		val conf = new SparkConf().setMaster("local[*]").setAppName("WatermarkDemo")
		val spark = SparkSession.builder().config(conf).getOrCreate()
		// 日志级别
		spark.sparkContext.setLogLevel("ERROR")
		import spark.implicits._
		// ==================== 业务处理 ====================
		// 接收数据
		val lines: DataFrame = spark.readStream
			.format("socket")
			.option("host", "localhost")
			.option("port", 9999)
			.load()
		// 处理数据
		val words: Dataset[WordCount] = lines.as[String]
			.map(row => {
				val fields = row.split(",")
				val ts = LocalDateTime.parse(fields(1), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
					.toInstant(ZoneOffset.ofHours(8)).toEpochMilli()
				WordCount(fields(0), new Timestamp(ts))
			})
		// 按窗口和单词对数据分组，并计算每组的计数
		val windowWordCounts = words
			// window(列, 窗口时间, 滑动时间)
			.groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
			.count()
		// 输出数据
		val query = windowWordCounts.writeStream
			.outputMode("update")
			// 触发器策略，根据处理时间的间隔周期性地运行查询。如果 interval 为 0，查询将尽可能快地运行
			.trigger(Trigger.ProcessingTime(0))
			.format("console")
			.start()
		// 调用 awaitTermination 防止应用退出
		query.awaitTermination()
	}
}