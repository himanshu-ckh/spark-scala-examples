package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object DataFrameFromCSVFile extends App {

  val spark = createSparkSession()

  val df1 = spark.read.csv("Input\\2010-summary.csv")
  df1.show()

  val df2 = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("Input\\2010-summary.csv")
  df2.show()

  val schema = new StructType(Array(
    StructField("destinationCountry", StringType, nullable = true),
    StructField("originCountry", StringType, nullable = true),
    StructField("count", DoubleType, nullable = true)
  ))

  val schema2 = new StructType()
    .add(StructField("destinationCountry", StringType, nullable = true))
    .add(StructField("originCountry", StringType, nullable = true))
    .add(StructField("count", DoubleType, nullable = true))

  val schema3 = StructType(
    StructField("destinationCountry", StringType, nullable = true) ::
    StructField("originCountry", StringType, nullable = true) ::
    StructField("count", DoubleType, nullable = true) :: Nil
  )

  val df3 = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).schema(schema).csv("Input\\2010-summary.csv")
  val df4 = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).schema(schema2).csv("Input\\2010-summary.csv")
  val df5 = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).schema(schema3).csv("Input\\2010-summary.csv")

  df5.show(false)
}
