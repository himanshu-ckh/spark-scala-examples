package com.himanshu.main.RDD

import com.himanshu.main.MainPackage.SparkMain._

object createEmptyRDD {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val rdd = spark.sparkContext.emptyRDD
    val rddString = spark.sparkContext.emptyRDD[String]

    println(rdd)
    println(rddString)

    val numberOfPartitions = rdd.getNumPartitions
    println(numberOfPartitions)

    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println(rdd2.getNumPartitions)

    type dataType = (String, Int)
    val pairRDD = spark.sparkContext.emptyRDD[dataType]
    println(pairRDD)

  }
}
