package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object createEmptyDataFrame extends App {
  val spark = createSparkSession()

  val schema = StructType(
    StructField("firstName", StringType, nullable = true) ::
    StructField("lastName", IntegerType, nullable = false) ::
    StructField("middleName", IntegerType, nullable = false) :: Nil)

  val colSeq = Seq("firstName","lastName","middleName")

  case class Name(firstName: String, lastName: String, middleName: String)

  val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  import spark.implicits._
  //Using Implicit encoder and case class
  Seq.empty[Name].toDF()

  //Using Simple implicit encoder
  Seq.empty[(String, String, String)].toDF(colSeq:_*)

  spark.emptyDataFrame

}
