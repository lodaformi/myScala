package com.loda.window

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author loda
 * @Date 2023/4/3 19:57
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello9KafkaOffset {
	def main(args: Array[String]): Unit = {
		//消除冗余信息提示
		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)

		val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Hello9KafkaOffset")
		val streamingContext = new StreamingContext(sparkConf, Seconds(5))
		//配置信息
		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "yjx_bigdata_kafka_new",
			"auto.offset.reset" -> "earliest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val topics = Array("sparkStreaming")
		//开始创建Kafka
		val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
			PreferConsistent, Subscribe[String, String](topics, kafkaParams))

		kafkaStream.foreachRDD(rdd=>{
			println("-----rdd.partitions.size------------------" + rdd.partitions.size)
			// 获取当前批次的offset数据
			val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			rdd.foreachPartition { iter =>
				val offsetRange: OffsetRange = offsetRanges(TaskContext.get.partitionId)

				println("[topic]" + offsetRange.topic +
					"[partition]" + offsetRange.partition +
					"[fromOffset]" + offsetRange.fromOffset +
					"[untilOffset]" + offsetRange.untilOffset)
			}
			// 在kafka 自身维护提交
			kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
		})

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}
