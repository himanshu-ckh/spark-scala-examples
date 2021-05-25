package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
object ArrayToColumns {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val arrayData = Seq(
      Row("James", List("Java", "Scala", "C++")),
      Row("Roy", List("Java", "Scala", "Spark")),
      Row("Him", List("Java", "Scala"))
    )

    val arraySchema = new StructType(
      Array(
        StructField("name", StringType),
        StructField("subjects", ArrayType(StringType))
      )
    )

    val newArraySchema = new StructType().add("name", StringType).add("subjects", ArrayType(StringType))

    val arrayDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), newArraySchema)
    arrayDF.printSchema()
    arrayDF.show()


    val arrayArrayData = Seq(
      Row("James", List(List("Java", "Scala", "C++"), List("Spark", "Java"))),
      Row("Michael", List(List("Spark", "Java", "C++"), List("Spark", "Java"))),
      Row("Robert", List(List("CSharp", "VB"), List("Spark", "Python")))
    )

    val arrayArraySchema = new StructType().add("name", StringType).add("subjects", ArrayType(ArrayType(StringType)))

    val arrayArrayDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema)

    arrayArrayDF.printSchema()
    arrayArrayDF.show()

    val df2 = arrayArrayDF.select(
      arrayArrayDF("name") +: (0 until 2).map(i => arrayArrayDF("subjects")(i).alias(s"LanguageKnow$i")): _*
    )

    df2.show(false)
  }
}
