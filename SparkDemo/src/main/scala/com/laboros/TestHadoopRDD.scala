package com.laboros

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.hadoop.io.{LongWritable,Text};
import org.apache.spark.HashPartitioner

object TestHadoopRDD {
  def main(args: Array[String]): Unit = {
    
    
     //Step-1 : SparkConf
    val sparkConf = new SparkConf();
    sparkConf.setAppName("TestDataLoad");
    sparkConf.setMaster("local");
    sparkConf.set("spark.submit.deployMode","client");
    //App Name : WebUI -- name 
    //Step-2: SparkContext
    val sc = new SparkContext(sparkConf);
    
    //Create a RDD 
    //LocalFile = file:///
    //HDFS  = hdfs:/ipaddress:port
    //s3 = s3n://ipadress_of_buket/bucket/files or s3
    
    val inputFile = "hdfs:/user/trainings/custs";
    
    val hadoopFileRDD = sc.hadoopFile(inputFile, classOf[TextInputFormat]
    , classOf[LongWritable], classOf[Text], 10);
     val hadoopData = hadoopFileRDD.map(t => (t._1.get,t._2.toString));
    val partData= hadoopData.partitionBy(new HashPartitioner(10))
    partData.saveAsTextFile("hdfs:/user/trainings/PARTITIONEDDATA");
  }
}