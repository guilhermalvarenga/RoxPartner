# Databricks notebook source
from pyspark.sql.session import SparkSession

# COMMAND ----------

file_location = "/FileStore/tables"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

# COMMAND ----------

Sales_SpecialOfferProduct = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f'{file_location}/Sales_SpecialOfferProduct.csv')

Sales_SpecialOfferProduct.registerTempTable("Sales_SpecialOfferProduct")

display(Sales_SpecialOfferProduct)

# COMMAND ----------

Sales_SalesOrderHeader = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f'{file_location}/Sales_SalesOrderHeader.csv')

Sales_SalesOrderHeader.registerTempTable("Sales_SalesOrderHeader")

display(Sales_SalesOrderHeader)

# COMMAND ----------

Sales_SalesOrderDetail = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f'{file_location}/Sales_SalesOrderDetail.csv')

Sales_SalesOrderDetail.registerTempTable("Sales_SalesOrderDetail")

display(Sales_SalesOrderDetail)

# COMMAND ----------

Sales_Customer = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f'{file_location}/Sales_Customer.csv')

Sales_Customer.registerTempTable("Sales_Customer")

display(Sales_Customer)

# COMMAND ----------

Production_Product = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f'{file_location}/Production_Product.csv')

Production_Product.registerTempTable("Production_Product")

display(Production_Product)

# COMMAND ----------

Person_Person = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f'{file_location}/Person_Person-4.csv')

Person_Person.registerTempTable("Person_Person")

display(Person_Person)

# COMMAND ----------

# QUESTÃO 1

# COMMAND ----------

%sql
select count(SalesOrderID)
from Sales_SalesOrderDetail

# COMMAND ----------

# QUESTÃO 2


# COMMAND ----------

%sql
select PP.Name, sum(SOD.OrderQty) OrderQty, PP.DaysToManufacture
from Sales_SalesOrderDetail SOD
inner join Production_Product PP on PP.ProductID = SOD.ProductID
inner join Sales_SpecialOfferProduct SOP on SOP.ProductID = SOD.ProductID
group by PP.Name, PP.DaysToManufacture
order by 2 desc
limit 3

# COMMAND ----------

# QUESTÃO 3

# COMMAND ----------

%sql
select CONCAT(PP.FirstName," ",PP.MiddleName," ",PP.LastName) as FullName, count(SC.StoreID) as CountStore
from Sales_Customer SC
inner join Person_Person PP on PP.BusinessEntityID = SC.CustomerID
inner join Sales_SalesOrderHeader SOH on SC.CustomerID = SOH.CustomerID
group by CONCAT(PP.FirstName," ",PP.MiddleName," ",PP.LastName)
order by count(SC.StoreID) desc

# COMMAND ----------

# QUESTÃO 4

# COMMAND ----------

%sql
select sum(SOD.OrderQty) as SumProducts, SOD.ProductID, SOH.OrderDate
from Sales_SalesOrderHeader SOH
inner join Sales_SalesOrderDetail SOD on SOH.SalesOrderID = SOD.SalesOrderID
left join Production_Product PP on PP.ProductID = SOD.ProductID
group by SOD.ProductID, SOH.OrderDate
order by sum(SOD.OrderQty) desc

# COMMAND ----------

# QUESTÃO 5

# COMMAND ----------

%sql
select SalesOrderID, OrderDate, TotalDue
from Sales_SalesOrderHeader
where OrderDate between '2011-09-01' and '2011-09-31'
and TotalDue > '1000'
order by TotalDue desc
