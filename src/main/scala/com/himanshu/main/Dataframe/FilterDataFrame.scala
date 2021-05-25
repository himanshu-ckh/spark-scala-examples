package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.functions.{array_contains, col, explode}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object FilterDataFrame extends App {
  val spark = createSparkSession()

  val arrayStructureData = Seq(
    Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
    Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
    Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
    Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
    Row(Row("Jen","Mary","Brown"),List("CSharp","VB"), null,"M"),
    Row(Row("Mike","Mary","Williams"),List("Python","VB"), null,"M")
  )

  val schema = new StructType()
    .add("name", new StructType()
    .add("firstName", StringType)
      .add("middleName", StringType)
      .add("lastName", StringType)
    )
    .add("language", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData), schema)
  df.printSchema()
  df.show(false)

  df.select("name.*").show(false)
  df.select(explode(col("language"))).show(false)
  df.filter(col("state") === "OH").show(false)

  df.filter(col("name.lastName") === "Williams").show(false)

  df.filter(array_contains(col("language"), "Java")).show(false)

  df.filter(col("state").isNull).show(false)

}
