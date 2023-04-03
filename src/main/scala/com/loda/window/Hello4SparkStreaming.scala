package com.loda.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author loda
 * @Date 2023/4/3 16:23
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello4SparkStreaming {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)

		val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Hello4SparkStreaming")

		val streamingContext = new StreamingContext(sparkConf, Seconds(5))
		streamingContext.checkpoint("checkpoint")

		val linesDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

		val s20DStream: DStream[String] = linesDStream.window(Seconds(20), Seconds(10))
		val resS20: DStream[(String, Int)] = s20DStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

		resS20.print()

		streamingContext.start()
		streamingContext.awaitTermination()

	}
}
