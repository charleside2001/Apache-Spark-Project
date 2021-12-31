# Databricks notebook source
# DBTITLE 1,Find a Directory with CSVs
# MAGIC %fs ls /databricks-datasets/online_retail/data-001/

# COMMAND ----------

# specify path
path = "/databricks-datasets/online_retail/data-001/data.csv"

# load as text
data = spark.read.csv(path) 

# show sample
data.take(20)

# COMMAND ----------

# DBTITLE 1,Read in Data to DataFrame with Column Headers
# read in file using csv format
df = spark.read.load(path,
                    format='com.databricks.spark.csv', 
                    header='true',
                    inferSchema='true')

# show 20 rows
display(df)

# COMMAND ----------

# DBTITLE 1,Show Countries
# For this we'll need a few functions
display( # shows the results in a grid
   df 
    .select("Country") # chooses just the 1 column
    .distinct() # removes duplicates
    .orderBy("Country") # sorts results in ascending
)
        

# COMMAND ----------

