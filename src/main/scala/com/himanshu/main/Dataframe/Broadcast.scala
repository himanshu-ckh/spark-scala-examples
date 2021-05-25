package com.himanshu.main.Dataframe

import com.himanshu.main.MainPackage.SparkMain.createSparkSession

object Broadcast extends App {
  val spark = createSparkSession()

  val states = Map(("NY", "New York"), ("FL", "Florida"))
  val countries = Map(("USA", "United states of America"), ("IN", "India"))

  val broadcastStates = spark.sparkContext.broadcast(states)
  val broadcastCounties = spark.sparkContext.broadcast(countries)

  val data = Seq(("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )

  val columns = Seq("firstname","lastname","country","state")
  import spark.sqlContext.implicits._
  val df = data.toDF(columns:_*)
  println(broadcastCounties.value)
  val df2 = df.map(row => {
    val country = row.getString(2)
    val state = row.getString(3)

    val fullCountry = broadcastCounties.value.get(country)
    val fullState = broadcastStates.value.get(state)

    (row.getString(0), row.getString(1), fullCountry, fullState)
  }).toDF(columns: _*)

  df2.show(false)
}
