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

customers.createOrReplaceTempView("customers")

// COMMAND ----------

val getCustomerType = (credit: Int) => {
  if(credit < 10000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT address as City, getCustomerType(credit) AS CustomerType, COUNT(customerid) AS NoOfCustomers
// MAGIC FROM customers
// MAGIC GROUP BY CustomerType, address
// MAGIC ORDER BY CustomerType, City

// COMMAND ----------

case class Product(productid: BigInt, title: String, unitsinstock: BigInt, unitprice: BigInt, itemdiscount: BigInt)

val products = 
  spark
    .read
    .option("multiline", true)
    .json("/mnt/data/Products/*.json")
    .as[Product]

// COMMAND ----------

val getOrderAmount = (units: Int, unitPrice: Int, itemdiscount: Int) => {
  val total = (units * unitPrice)
  val discount = ((total * itemdiscount) / 100).asInstanceOf[Int]
  
  (total - discount).asInstanceOf[Int]
}

spark.udf.register("getOrderAmount", getOrderAmount)

// COMMAND ----------

case class Order(orderid: Int, orderdate: String, customer: Int, product: Int, billingaddress: String, units: Int, remarks: String)

val orders = 
  spark
    .read
    .option("inferSchema", true)
    .option("header", true)
    .option("sep", ",")
    .csv("/mnt/data/Orders/*.csv")
    .as[Order]

// COMMAND ----------

orders.printSchema

// COMMAND ----------

customers.createOrReplaceTempView("customers")
products.createOrReplaceTempView("products")
orders.createOrReplaceTempView("orders")

// COMMAND ----------

val sqlStatement = 
"""SELECT o.orderid AS OrderId, o.orderdate AS OrderDate, c.fullname AS CustomerName, p.title AS ProductTitle,
  c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
  getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
  p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
  o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
FROM orders o
INNER JOIN customers c ON c.customerid = o.customer
INNER JOIN products p ON p.productid = o.product
WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
ORDER BY OrderAmount"""

val processedOrders = spark.sql(sqlStatement)

processedOrders
  .write
  .parquet("/mnt/data/optimized-processed-orders/10092019")

// COMMAND ----------

val parquetProcessedOrders = 
  spark
    .read
    .parquet("/mnt/data/optimized-processed-orders/10092019")

display(parquetProcessedOrders)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE ProcessedOrders
// MAGIC USING PARQUET
// MAGIC LOCATION "/mnt/data/optimized-processed-orders/10092019"

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT CustomerLocation, SUM(OrderAmount) AS TotalOrderAmount
// MAGIC FROM ProcessedOrders
// MAGIC GROUP BY CustomerLocation
// MAGIC ORDER BY CustomerLocation