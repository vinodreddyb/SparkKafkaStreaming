package com.vinod.spark.streaming

/**
  * Created by vinod on 2016-11-16.
  */

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object DirectKafkaWordCount {
  def main(args: Array[String]) {
    //val Array(brokers, topics) = argsvinod123


    val brokers = "localhost:9092"
    val topics = "test"
    // var offsetRanges = Array[OffsetRange]()
    val offsetRanges = Array(
      OffsetRange("test", 0, 110, 220))
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // ssc.checkpoint("/checkpoint")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"auto.offset.reset" -> "smallest")
   /* val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)*/


    val messages = KafkaSource.kafkaStream[String, String, StringDecoder, StringDecoder](
      ssc, brokers, new ZooKeeperOffsetsStore("localhost","/home/test"),topics)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }



}