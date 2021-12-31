# Databricks notebook source
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

# DBTITLE 1,Show Top Products in the UK
display(
  df
    .select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total"))
    .groupBy("Country", "Description")
    .sum()
    .filter(df["Country"]=="United Kingdom")
    .sort("sum(Total)", ascending=False)
  )

# COMMAND ----------

# DBTITLE 1,Calculate Product Sales by Country
r1 = df.select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total"))

display(r1)


# COMMAND ----------

# DBTITLE 1,Save Results as Table
r1.write.saveAsTable("product_sales_by_country")

# COMMAND ----------

