package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, CalendarIntervalType, MapType, NullType, ObjectType, StringType, StructType}

object flattenStructSchema extends App {
  val spark = createSparkSession()

  val structureData = Seq(
    Row(Row("James ","","Smith"),Row(Row("CA","Los Angles"),Row("CA","Sandiago"))),
    Row(Row("Michael ","Rose",""),Row(Row("NY","New York"),Row("NJ","Newark"))),
    Row(Row("Robert ","","Williams"),Row(Row("DE","Newark"),Row("CA","Las Vegas"))),
    Row(Row("Maria ","Anne","Jones"),Row(Row("PA","Harrisburg"),Row("CA","Sandiago"))),
    Row(Row("Jen","Mary","Brown"),Row(Row("CA","Los Angles"),Row("NJ","Newark")))
  )

  val structureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("address",new StructType()
      .add("current",new StructType()
        .add("state",StringType)
        .add("city",StringType))
      .add("previous",new StructType()
        .add("state",StringType)
        .add("city",StringType)))


  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),structureSchema)
  df.printSchema()
  df.show(false)

  val df2 = df.select(col("name.*"),
    col("address.current.*"),
    col("address.previous.*"))
    .toDF("fname","mname","lname","currAddState", "currAddCity","prevAddState","prevAddCity")
  df2.show(false)

  def flattenStructSchema(schema: StructType, prefix: String =null): Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if(prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType =>flattenStructSchema(st, columnName)
        case _ =>Array(col(columnName).as(columnName.replace(".", "_")))
      }
    })
  }

  val df3 = df.select(flattenStructSchema(df.schema):_*)
  df3.show(false)
}
