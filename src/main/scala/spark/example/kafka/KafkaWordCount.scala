/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
*/

package spark.example.kafka

import java.util.Properties

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._



/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
*      my-consumer-group topic1,topic2 1`
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf(true).setAppName("KafkaWordCount").set("spark.cassandra.connection.host", "127.0.0.1")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)

	val stream = lines.map{ line => 
	val Array(ip, size) = LogFormat(line)
	(ip, size)
	}.saveToCassandra("streaming_test", "words", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
  def LogFormat(line: String) :Array[String]= {
	val patten = """^(\d+\.\d+\.\d+\.\d+)\s+\-\s+\-\s+\[(.*)\s+.*\]\s+\"(.*)\s+(.*)\s+(.*)\"?\s+(\d+)\s+(\d+)\s+\"(.*)\"?\s\"(.*)\"\s(.*)\s+(\d+)$""".r
	val Matches = patten.findFirstMatchIn(line)
	Matches match {
		case Some(m) =>
			val result = Array(m.group(1), m.group(7))
			result
		case None =>
			val result = new Array[String](0)
			result 
		}
  }
}
