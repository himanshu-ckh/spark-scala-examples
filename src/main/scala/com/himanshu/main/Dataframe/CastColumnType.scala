package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}

object CastColumnType extends App {
  val spark = createSparkSession()

  val sampleData = Seq(
    Row("James", 34, "2006-01-01", "true", "M", 3000.60),
    Row("Michael", 33, "1980-01-10", "true", "F", 3300.80),
    Row("Robert", 37, "06-01-1992", "false", "M", 5000.50)
  )

  val simpleSchema = StructType(Array(
    StructField("firstName", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("jobStartDate", StringType, nullable = true),
    StructField("isGraduated", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("salary", DoubleType, nullable = true)
  ))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(sampleData), simpleSchema)
  df.printSchema()

  val df2 = df.withColumn("age", col("age").cast(StringType)).withColumn("isGraduated", col("isGraduated").cast(BooleanType))
  df2.printSchema()

  val df3 = df2.selectExpr("cast(age as int) age",
    "cast(isGraduated as string) isGraduated",
    "cast(jobStartDate as string) jobStartDate")
  df3.printSchema()

  val castDF = df.select(df.columns.map{
    case column@"age" =>
      col(column).cast("String").as(column)
    case column@"salary" =>
      col(column).cast("String")
    case column =>
      col(column)
  }: _*)

  castDF.printSchema()

  //This one is additional which contains how to not to truncate a column
  import spark.implicits._
  val columns = Seq("Seqno","Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."))
  val df4 = data.toDF(columns:_*)
  df4.show(false)

}
