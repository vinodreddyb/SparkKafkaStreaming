package com.vinod.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.client.{HBaseAdmin, Mutation, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * This class stream the messages from the kafka using direct stream api and push the word count aggregation to
  * Hbase table.
  *
  * If the Hbase table is existed then update the word count
  * else creates new table and insert the results
  */

object DirectKafkaSparkHbaseExample {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val brokers = "localhost:9092"
    val topics = "wc"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    /* create direct kafka stream */
    val messages = createCustomDirectKafkaStream(ssc,kafkaParams,"localhost:2182","/kafka", topicsSet)
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    saveToHbaseTable(wordCounts,ssc)
    ssc.start()
    ssc.awaitTermination()
  }

  def saveToHbaseTable(wordCounts: DStream[(String, Long)], ssc: StreamingContext): Unit = {
   wordCounts.foreachRDD(rdd => {

      val hbaseTableName = "wordcount"
      val hbaseColumnName = "aggregate"
      //Creates the HBase confs
      val hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", "localhost")
      hconf.set("hbase.zookeeper.property.clientPort", "2183")
      hconf.set("hbase.defaults.for.version.skip", "true")
      hconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
      hconf.setClass("mapreduce.job.outputformat.class", classOf[TableOutputFormat[String]], classOf[OutputFormat[String, Mutation]])
      val admin = new HBaseAdmin(hconf)

      //If table already exists, then lets read back and update it
      if (admin.isTableAvailable(hbaseTableName)) {
        hconf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
        hconf.set(TableInputFormat.SCAN_COLUMNS, "CF:" + hbaseColumnName + " CF:batch")
        val check = ssc.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map {
          case (key, row) => {
            val rowkey = Bytes.toString(key.get())
            val valu = Bytes.toString(row.getValue(Bytes.toBytes("CF"), Bytes.toBytes(hbaseColumnName)))
            (rowkey, valu.toLong)
          }
        }

        //Lets union the stream data with previous hbase data.
        val jdata = (check ++ rdd).reduceByKey(_ + _)

        jdata.map(valu => (new ImmutableBytesWritable, {
          val record = new Put(Bytes.toBytes(valu._1))
          record.add(Bytes.toBytes("CF"), Bytes.toBytes(hbaseColumnName), Bytes.toBytes(valu._2.toString))
          record
        }
          )
        ).saveAsNewAPIHadoopDataset(hconf)


        //Let’s create the table and push the data into it.
      }else{

        val tab = TableName.valueOf(hbaseTableName)
        val tabledesc = new HTableDescriptor(tab)
        tabledesc.addFamily(new HColumnDescriptor("CF"))
        admin.createTable(tabledesc)
        rdd.map(valu => (new ImmutableBytesWritable, {
          val record = new Put(Bytes.toBytes(valu._1))
          record.add(Bytes.toBytes("CF"), Bytes.toBytes(hbaseColumnName), Bytes.toBytes(valu._2.toString))
          record
        }
          )
        ).saveAsNewAPIHadoopDataset(hconf)


      }
    })
  }

  def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String
                                    , zkPath: String, topics: Set[String]): InputDStream[(String, String)] = {
    val topic = topics.last //TODO only for single kafka topic r(ight now
    val zkClient = new ZkClient(zkHosts, 30000, 30000)
    val storedOffsets = readOffsets(zkClient,zkHosts, zkPath, topic)
    val kafkaStream = storedOffsets match {
      case None => // start from the latest offsets
        println("Starting fresh---")
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) => // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        println("Starting from offsets ---" + fromOffsets)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,(String,String)](ssc, kafkaParams, fromOffsets,messageHandler)
    }
    // save the offsets
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient,zkHosts, zkPath, rdd))
    kafkaStream
  }

    /*
     Read the previously saved offsets from Zookeeper
      */
    private def readOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, topic: String):
    Option[Map[TopicAndPartition, Long]] = {
      println("Reading offsets from Zookeeper")
      val stopwatch = new Stopwatch()
      val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
      offsetsRangesStrOpt match {
        case Some(offsetsRangesStr) =>
          println(s"Read offset ranges: ${offsetsRangesStr}")
          val offsets = offsetsRangesStr.split(",")
            .map(s => s.split(":"))
            .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
            .toMap
          println("Done reading offsets from Zookeeper. Took " + stopwatch)
          Some(offsets)
        case None =>
          println("No offsets found in Zookeeper. Took " + stopwatch)
          None
      }
    }

    private def saveOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, rdd: RDD[_]): Unit = {
      println("Saving offsets to Zookeeper")
      val stopwatch = new Stopwatch()
      val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsRanges.foreach(offsetRange => println(s"Using ${offsetRange}"))
      val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
        .mkString(",")
      ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
      //logger.info("Done updating offsets in Zookeeper. Took " + stopwatch)
    }

    class Stopwatch {
      private val start = System.currentTimeMillis()
      override def toString() = (System.currentTimeMillis() - start) + " ms"
    }


  }