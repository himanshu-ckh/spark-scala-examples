package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.SaveMode

object AvroExample {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val data = Seq(("James ", "", "Smith", 2018, 1, "M", 3000),
      ("Michael ", "Rose", "", 2010, 3, "M", 4000),
      ("Robert ", "", "Williams", 2010, 3, "M", 4000),
      ("Maria ", "Anne", "Jones", 2005, 5, "F", 4000),
      ("Jen", "Mary", "Brown", 2010, 7, "", -1)
    )

    val columns = Seq("firstname", "middlename", "lastname", "dob_year",
      "dob_month", "gender", "salary")
    import spark.implicits._
    val df = data.toDF(columns: _*)
    df.show()

    df.write.partitionBy("dob_year", "dob_month").format("parquet").mode(SaveMode.Overwrite).save("C:\\Users\\himan\\IdeaProjects\\ScalaSpark\\Out\\DataFrame\\AvroExample")
//    df.write.partitionBy("dob_year", "dob_month").format("json").mode(SaveMode.Overwrite).save("C:\\Users\\himan\\IdeaProjects\\ScalaSpark\\Out\\DataFrame\\JSON")
  }
}
