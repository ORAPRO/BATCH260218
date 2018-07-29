package com.laboros.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession;

object SparkSQLDemo 
{
  def main(args: Array[String]): Unit = {
    
    //Step -1 : Create sparksession
    var sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.set("spark.submit.deployMode", "client");
    sparkConf.setAppName("SparkSQLDemo");
    
    
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();
    
    //implicits
    import spark.implicits._;
    
  }
}