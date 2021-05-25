package com.himanshu.main.RDD

import com.himanshu.main.MainPackage.SparkMain._

object createRDD {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val rdd = spark.sparkContext.parallelize(Seq(("Java", 20000), ("Scala", 3000)))
    rdd.foreach(println)

    val rdd2 = rdd.map(row => {
      (row._1, row._2 + 100)
    })

    rdd2.foreach(println)

    val rdd3 = spark.range(10).toDF().rdd
    rdd3.foreach(println)
  }
}
