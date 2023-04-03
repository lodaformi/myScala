package com.loda.window

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author loda
 * @Date 2023/4/3 17:18
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello7Kafka {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)

		val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Hello7Kafka")
		val streamingContext = new StreamingContext(sparkConf, Seconds(2))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "yjx_kafka",
			"auto.offset.reset" -> "earliest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val topics = Array("sparkStreaming")

		val kafkaStream: InputDStream[ConsumerRecord[String, String]] =
			KafkaUtils.createDirectStream(streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

		kafkaStream.map(_.value()).foreachRDD(rdd=>rdd.foreach(println))

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}
