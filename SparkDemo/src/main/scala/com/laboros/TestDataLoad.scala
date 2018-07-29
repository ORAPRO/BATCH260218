package com.laboros


import org.apache.spark.{SparkConf, SparkContext};

object TestDataLoad 
{
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
    
    val inputFile = "file:///home/trainings/SAIWS/BATCHES/BATCH220218/Customer/custs";
    
    val inputRDD = sc.textFile(inputFile, 3);
    
    inputRDD.glom.collect.foreach(println)
    
  }
}