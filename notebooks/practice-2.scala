// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Load Data from DLS to ADB
// MAGIC ## Processing Data

// COMMAND ----------

import spark.implicits._

case class Customer (customerid: Int, fullname: String, address: String, credit: Int, status: Boolean, remarks: String)

val customersCsvLocation = "/mnt/data/Customers/*.csv"
val customers = 
  spark
    .read
    .option("inferSchema", true)
    .option("header", true)
    .option("sep", ",")
    .csv(customersCsvLocation)
    .as[Customer]