package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession
import org.apache.spark.sql.functions.{col, explode}

case class Employee(firstName:String,lastName:String, email:String,salary:Int)
case class Department(id:Int,name:String)
case class DepartmentWithEmployees(department: Department, employee: Seq[Employee])

object DataFrameWithComplexDSL extends App {
  val spark = createSparkSession()

  val department1 = Department(123456, "Computer Science")
  val department2 = Department(789012, "Mechanical Engineering")
  val department3 = Department(345678, "Theater and Drama")
  val department4 = Department(901234, "Indoor Recreation")

  //Create the Employees

  val employee1 = Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
  val employee2 = Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
  val employee3 = Employee("matei", "", "no-reply@waterloo.edu", 140000)
  val employee4 = Employee("", "wendell", "no-reply@berkeley.edu", 160000)

  val departmentWithEmployees1 = DepartmentWithEmployees(department1, List(employee1, employee2))
  val departmentWithEmployees2 = DepartmentWithEmployees(department2, Seq(employee3, employee4))
  val departmentWithEmployees3 = DepartmentWithEmployees(department3, List(employee1, employee4))
  val departmentWithEmployees4 = DepartmentWithEmployees(department4, List(employee2, employee3))

  val data1 = Seq(departmentWithEmployees1,departmentWithEmployees2)

  val data2 = Seq(departmentWithEmployees3,departmentWithEmployees4)

  import spark.implicits._

  val df = spark.createDataFrame(data1)
  val df2 = spark.createDataFrame(data2)
  df2.printSchema()

  val finalDF = df.union(df2)
  val res1 = finalDF.select("department.*")
  res1.show(false)

  val res2 = finalDF.select(explode(col("employee"))).select("col.*")
  res2.show(false)
}
