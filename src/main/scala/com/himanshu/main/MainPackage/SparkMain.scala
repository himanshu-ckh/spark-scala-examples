package com.himanshu.main.MainPackage

import org.apache.spark.sql.SparkSession

object SparkMain {
  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder().appName("Learn").master("local[*]").getOrCreate()
    spark
  }

}
