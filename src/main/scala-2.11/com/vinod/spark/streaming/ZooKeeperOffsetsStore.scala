package com.vinod.spark.streaming


import kafka.common.TopicAndPartition
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

class ZooKeeperOffsetsStore(zkHosts: String, zkPath: String) extends OffsetsStore  {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer)

  // Read the previously saved offsets from Zookeeper
  override def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]] = {

    val stopwatch = new Stopwatch()

    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)

    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        println(s"Read offset ranges: ${offsetsRangesStr}")

        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap

        println("Done reading offsets from ZooKeeper. Took " + stopwatch)

        Some(offsets)
      case None =>
        println("No offsets found in ZooKeeper. Took " + stopwatch)
        None
    }

  }

  // Save the offsets back to ZooKeeper
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  override def saveOffsets(topic: String, rdd: RDD[_]): Unit = {

    println("Saving offsets to ZooKeeper")
    val stopwatch = new Stopwatch()

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => println(s"Using ${offsetRange}"))

    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    println(s"Writing offsets to ZooKeeper: ${offsetsRangesStr}")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)

    println("Done updating offsets in ZooKeeper. Took " + stopwatch)

  }

}
