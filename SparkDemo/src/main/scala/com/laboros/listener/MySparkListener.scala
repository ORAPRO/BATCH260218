package com.laboros.listener

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerExecutorAdded
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.kafka.clients.producer.KafkaProducer
import com.laboros.producer.ProducerUtil
import org.apache.kafka.clients.producer.ProducerRecord

//import org.apache.kafka.clients.producer.KafkaProducer

//spark-submit --verbose --master local --deploy-mode client --conf spark.driver.extraClassPath=/home/edureka/SAIWS/BATCHES/mysql-connector-java-5.1.45-bin.jar:/usr/lib/kafka_2.12-0.11.0.0/libs/kafka-clients-0.11.0.0.jar --repositories https://repo.maven.apache.org/maven2 --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.0.0 --conf spark.extraListeners=com.laboros.listener.MySparkListener  --class com.laboros.WordCountDriver sparkdemo_2.11-1.0.jar file:///home/edureka/SAIWS/BATCHES/BATCH220216/SparkWS/SparkJars/WordCount.txt

class MySparkListener extends SparkListener {
  var appId: String = null;
  var producer: KafkaProducer[String, String] = null;
  val topic = "MyTopic";
  def publishEvent(event: SparkListenerEvent) {

    if (appId != null && topic != null) {
      if (producer == null) {
        producer = ProducerUtil.getProducer();
      }
      writeDataToProducer(topic, event, producer);
    }
  }

  def writeDataToProducer(topic: String, event: SparkListenerEvent, producer: KafkaProducer[String, String]) {

    val eventName = event.toString().substring(event.toString().indexOf("("));

    println("eventName" + eventName);

    eventName match {

      case "onApplicationStart" =>
        {
          val innerEvent = event.asInstanceOf[SparkListenerApplicationStart];
          producer.send(new ProducerRecord(topic, appId, innerEvent.appId + "," + innerEvent.appName))
        }
      case "onExecutorAdded" =>
        {
          val innerEvent = event.asInstanceOf[SparkListenerExecutorAdded];
          producer.send(new ProducerRecord(topic, appId, innerEvent.executorId + "," + innerEvent.executorInfo.executorHost))
        }
      case "onTaskStart" =>
        {
          val innerEvent = event.asInstanceOf[SparkListenerTaskStart];
          producer.send(new ProducerRecord(topic, appId, innerEvent.stageId + "," + innerEvent.taskInfo.speculative))

        }
      case _ => eventName
    }

  }
  override def onApplicationStart(event: SparkListenerApplicationStart) {

    appId = event.appId.get;

    if (appId != null) {
      if (producer == null) {
        producer = ProducerUtil.getProducer();
      }
      producer.send(new ProducerRecord(topic, appId, event.appId + "," + event.appName))

    }else{
      println("Application id is null");
    }
    println("onApplicationStart");
    //    publishEvent(event)
  }
  override def onExecutorAdded(event: SparkListenerExecutorAdded) {
    println("onExecutorAdded")
    if (appId != null) {
      if (producer == null) {
        producer = ProducerUtil.getProducer();
      }
      producer.send(new ProducerRecord(topic, appId, event.executorId + "," + event.executorInfo.executorHost))

    }else{
      println("Application id is null");
    }
    //    publishEvent(event)

  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    println("onApplicationEnd");
    if (producer != null) {
      producer.close();
      producer = null;
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart) {
    println("onTaskStart");
    if (appId != null) {
      if (producer == null) {
        producer = ProducerUtil.getProducer();
      }
      producer.send(new ProducerRecord(topic, appId, event.stageId + "," + event.taskInfo.speculative))
    }else{
      println("Application id is null");
    }
    //    publishEvent(event)

  }

}