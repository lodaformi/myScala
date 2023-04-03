package com.loda.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author loda
 * @Date 2023/4/3 16:54
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello6ForeachRdd {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)

		val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Hello5ForeachRdd")
		val streamingContext = new StreamingContext(sparkConf, Seconds(5))

		val linesDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)
		val res: DStream[(String, Int)] = linesDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
		res.foreachRDD(rdd=>{
			rdd.foreach(ele => {
				println(ele._1 + "====" + ele._2)
			})
		})

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}
