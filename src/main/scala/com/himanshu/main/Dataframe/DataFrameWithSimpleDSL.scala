package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.functions.col

object DataFrameWithSimpleDSL extends App {
  val spark = createSparkSession()

  val df1 = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("Input\\2010-summary.csv")

  //Where
  df1.select("*").where(df1("count") === 1).show()

  //Filter
  df1.filter(col("count") === 1).show()
}
