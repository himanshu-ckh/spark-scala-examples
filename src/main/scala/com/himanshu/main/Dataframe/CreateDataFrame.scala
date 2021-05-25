package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CreateDataFrame extends App{
  val spark = createSparkSession()

  val columns = Seq("language", "user_count")
  val data = Seq(("Java", "2000"), ("Scala", "100000"))

  val rdd = spark.sparkContext.parallelize(data)

  import spark.implicits._
  val df = rdd.toDF(columns:_*)

  val df2 = spark.createDataFrame(rdd).toDF("language", "user_count")

  val rowRDD = rdd.map(att => {
    Row(att._1, att._2)
  })

  val df3 = {
    spark.createDataFrame(rowRDD, new StructType(Array(StructField("language", StringType), StructField("user", StringType))))
  }

  df3.show()

  val df4 = data.toDF()
  df4.show()
}
