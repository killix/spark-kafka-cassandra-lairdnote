
package spark.c4hcdn.kafka

import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import java.util.Properties
import java.io.Serializable
import scala.collection.immutable.ListMap
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import spark.c4hcdn.kafka.AccessLog._
import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}
import java.text.SimpleDateFormat
import java.util.Locale
import com.redis._

object KafkaNetWork {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf()
		.setAppName("KafkaNetWork")
		//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		//.set("spark.kryo.registrator", "spark.c4hcdn.kafka.MyRegistrator")
	val sc = new SparkContext(sparkConf)
    val ssc =  new StreamingContext(sc, Seconds(2))
	val p = new AccessLogParser
    ssc.checkpoint("checkpoint")


    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)

	
	val stream = lines.flatMap(_.split("\\n"))
		.map(d => p.parseRecord(d))
		.map(x => (getDomainTime(x).toString,getSize(x).toInt))
		.reduceByKey(_ + _)
		.filter(_._1 != "0")
		.map(x => saveResults(x)).print()
	ssc.start()
    ssc.awaitTermination()
  }

  def saveResults(result: (String, Int)) = { 
			val (keys, size) = result
			println(keys)
			val Array(timestamp, domain) = keys.split("_")
			val r = new RedisClient("localhost", 6379)
			val u = uuid.toString.asInstanceOf[Serializable]
			val time = timestamp.toString.asInstanceOf[Serializable]
			val domain_to_string = domain.replace('.', '_')
			val d = domain_to_string.toString.asInstanceOf[Serializable]
			r.sadd(d, u)
			val text = timestamp + "_" + size
			r.sadd(u, text.toString.asInstanceOf[Serializable])
			(domain, u, size)
  }


  def uuid = java.util.UUID.randomUUID.toString

  def getSize(line: Option[AccessLogRecord]) = {
	  line match {
		  case Some(x) => x.bytesSent
		   case None => "0"
	  }
  }

  def regexDomain(request: String) = {
	  val regex = """.*http://([^/]+).*""".r
	  val m = regex.findFirstMatchIn(request)
	  m match {
		  case Some(x) => x.group(1)
		  case None => 0
	}
 }
 
 def getDomainTime(line: Option[AccessLogRecord]) = {
	line match {
		case Some(x) => 
			val request = regexDomain(x.request)
			val timestamp = javaDate(x.dateTime)
			val s = "_"
			val result = timestamp + s + request
			result
		case None => 0
	}
 }

 def javaDate(dateTime: String) = {
	 val Array(time, zone) = dateTime.split(" ")
	 val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
	 val resutl1 = dateFormat.parse(time)
	 val dp = resutl1.getTime()
	 dp
 }

}
