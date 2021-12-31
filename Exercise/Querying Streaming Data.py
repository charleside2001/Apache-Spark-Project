# Databricks notebook source
# DBTITLE 1,Download Sales Data
# MAGIC %sh 
# MAGIC curl -O https://raw.githubusercontent.com/bsullins/bensullins.com-freebies/master/sales_log.zip
# MAGIC file sales_log.zip

# COMMAND ----------

# MAGIC %sh unzip sales_log.zip

# COMMAND ----------

# DBTITLE 1,If already exists, check directory
# MAGIC %fs ls 'file:/databricks/driver/sales_log/'

# COMMAND ----------

# DBTITLE 1,Read in Data
from pyspark.sql.types import *

path = "file:/databricks/driver/sales_log/"

# create schema for data so stream processing is faster
salesSchema = StructType([
  StructField("OrderID", DoubleType(), True),
  StructField("OrderDate", StringType(), True),
  StructField("Quantity", DoubleType(), True),
  StructField("DiscountPct", DoubleType(), True),
  StructField("Rate", DoubleType(), True),
  StructField("SaleAmount", DoubleType(), True),
  StructField("CustomerName", StringType(), True),
  StructField("State", StringType(), True),
  StructField("Region", StringType(), True),
  StructField("ProductKey", StringType(), True),
  StructField("RowCount", DoubleType(), True),
  StructField("ProfitMargin", DoubleType(), True)])

# Static DataFrame containing all the files in sales_log
data = (
  spark
    .read
    .schema(salesSchema)
    .csv(path)
)


# create table so we can use SQL
data.createOrReplaceTempView("sales")

display(data)

# COMMAND ----------

# DBTITLE 1,Check Table
# MAGIC %sql select * from sales

# COMMAND ----------

# DBTITLE 1,Build a Chart
# MAGIC %sql 
# MAGIC select 
# MAGIC   ProductKey as Products,
# MAGIC   round(sum(SaleAmount)) as TotalSales
# MAGIC from sales
# MAGIC group by ProductKey
# MAGIC order by 2 desc
# MAGIC limit 100

# COMMAND ----------

# MAGIC %md # Streaming Setup
# MAGIC Now we'll try to convert the above analysis we completed to a streaming solution, by reading in each file one by one.

# COMMAND ----------

from pyspark.sql.functions import *

# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
streamingInputDF = (
  spark
    .readStream                       
    .schema(salesSchema)              # Set the schema for our feed
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .csv(path)
)

# Same query as staticInputDF
streamingCountsDF = (                 
  streamingInputDF
    .select("ProductKey", "SaleAmount")
    .groupBy("ProductKey")
    .sum()
)

# Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

# COMMAND ----------

# DBTITLE 1,Create Streaming Table
query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("sales_stream")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *  
# MAGIC from sales_stream
# MAGIC order by 2 desc
# MAGIC limit 100

# COMMAND ----------

