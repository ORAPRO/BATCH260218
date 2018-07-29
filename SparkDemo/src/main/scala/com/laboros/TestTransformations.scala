package com.laboros

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestTransformations {
  
  def main(args: Array[String]): Unit = {
    
    //Step: 1 : Create sparkcontext
    val sparkConf:SparkConf = new SparkConf();
    sparkConf.setAppName(TestTransformations.getClass.getName);
//    sparkConf.setMaster("local");
//    sparkConf.set("spark.submit.deployMode", "client");
    
    val sc:SparkContext = new SparkContext(sparkConf);
    
    //step : 2 : Create an RDD
    val inputRDD = sc.parallelize(1 to 20, 5);
    
    //step: 3 : Transformations 
    
    val coaleasceRDD = inputRDD.coalesce(3, false);
    
    val repartRDD = inputRDD.repartition(3);
    
    val scriptPath = "/home/edureka/SAIWS/BATCHES/BATCH220216/SparkWS/echo.sh";
    
    val pipeRDD = inputRDD.pipe(scriptPath);
    
    //Step: 4 : Apply Actions
    sc.setLogLevel("WARN"); //suppress the logging
    println("coalease");
    coaleasceRDD.glom().collect.foreach(println);
    sc.setLogLevel("INFO");
    println("repartition")
    repartRDD.glom().collect().foreach(println);
    println("Pipe RDD")
    pipeRDD.collect().foreach(println);
    
    
  }
}