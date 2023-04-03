package com.loda.window

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author loda
 * @Date 2023/4/3 20:24
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello10OffsetOther {
	def main(args: Array[String]): Unit = {
		//消除冗余信息提示
		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)

		val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Hello10OffsetOther")
		val streamingContext = new StreamingContext(sparkConf, Seconds(5))

		//配置信息
		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "yjx_bigdata_other",
			"auto.offset.reset" -> "earliest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		//假设从其他数据库读取了offsets了
		val offsets: Map[TopicPartition, Long] = Map(
			new TopicPartition("sparkStreaming", 0) -> 15L,
			new TopicPartition("sparkStreaming", 1) -> 12L,
			new TopicPartition("sparkStreaming", 2) -> 8L
		)

		val topics = Array("sparkStreaming")
		//开始创建Kafka
		val kafkaStream: InputDStream[ConsumerRecord[String, String]] =
			KafkaUtils.createDirectStream[String, String](streamingContext,
			PreferConsistent, Subscribe[String, String](topics, kafkaParams, offsets))


		kafkaStream.foreachRDD({ rdd =>
			println("-----rdd.partitions.size------------------" + rdd.partitions.size)
			// 获取当前批次的offset数据
			val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			rdd.foreachPartition { iter =>
				val offsetRange: OffsetRange = offsetRanges(TaskContext.get.partitionId)
				println("[topic]" + offsetRange.topic + "[partition]" + offsetRange.partition +
					"[fromOffset]" + offsetRange.fromOffset + "[untilOffset]"
					+ offsetRange.untilOffset)
			}
		})

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}
