package com.laboros

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

/**
 * This is the entry point for spark programs
 * Basic 4 steps
 * 
 * 1) Create spark Context
 * 2) Create an RDD using any 3 steps
 * 			a) Using parallelize
 * 			b) External Dataset
 * 			c) Existing RDD
 * 3) Basic Transformations ( Transformations are lazy)
 * 4) Action to trigger spark job job
 */
object WordCountDriver {
  
  def main(args: Array[String]): Unit = {
    
    //Step-1 : Create SparkContext
    val sparkConf = new SparkConf().setAppName(WordCountDriver.getClass.getName);
    sparkConf.setMaster("local");
    sparkConf.set("spark.submit.deployMode", "client");
    
    val sc = new SparkContext(sparkConf);
    
    //Step-2 : Create an RDD
    
//    val input = List("DEER RIVER RIVER","CAT BEAR RIVER","RIVER CAT BEAR");
    val input = args(0);
    
//    val inputRDD = sc.parallelize(input, 2);
    val inputRDD = sc.textFile(args(0), 2);
    
    //Step-3 : Apply transformations (lazy)
    val words = inputRDD.flatMap(iLine=>iLine.split(" "));
    
    val addOne = words.map(w=>(w,1));
    
    val wc = addOne.reduceByKey(_+_);
    
    //Step-4: Apply action
    wc.collect().foreach(println);
    
//    val sc:SparkContext = SparkS
    
  }
  
  
}