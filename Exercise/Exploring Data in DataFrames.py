# Databricks notebook source
# DBTITLE 1,Find a Directory with CSVs
# MAGIC %fs ls /databricks-datasets/online_retail/data-001/

# COMMAND ----------

# DBTITLE 1,Read in Data to DataFrame with Column Headers
# specify path
path = "/databricks-datasets/online_retail/data-001/data.csv"

# read in file using csv format
df = spark.read.load(path,
                    format='com.databricks.spark.csv', 
                    header='true',
                    inferSchema='true')

# show 20 rows
display(df)

# COMMAND ----------

# DBTITLE 1,Show DataFrame Schema
# take a look at our schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Select Just 1 Column
# show just the countries
df.select("Country").show()

# COMMAND ----------

# DBTITLE 1,Remove Duplicates from Column and Sort
# For this we'll need a few functions
display( # shows the results in a grid
   df 
    .select("Country") # chooses just the 1 column
    .distinct() # removes duplicates
    .orderBy("Country") # sorts results in ascending
)
        

# COMMAND ----------

# DBTITLE 1,Create and Aggregation - Calculate Order Totals
display(
  df
    .select(df["InvoiceNo"],df["UnitPrice"]*df["Quantity"])
    .groupBy("InvoiceNo")
    .sum()
  )

# COMMAND ----------

# DBTITLE 1,Inspect Results with Filter
df.filter(df["InvoiceNo"]==536596).show()

# COMMAND ----------

# DBTITLE 1,Show Top 10 Products in the UK
display(
  df
    .select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total"))
    .groupBy("Country", "Description")
    .sum()
    .filter(df["Country"]=="United Kingdom")
    .sort("sum(Total)", ascending=False)
    .limit(10)
  )

# COMMAND ----------

