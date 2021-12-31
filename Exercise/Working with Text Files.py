# Databricks notebook source
# DBTITLE 1,Let's browse the file system to start
# MAGIC %fs ls

# COMMAND ----------

# DBTITLE 1,Now checkout some sample data
# MAGIC %fs ls /databricks-datasets/bikeSharing/README.md

# COMMAND ----------

# DBTITLE 1,Next, read in a file and count the lines in a document
path = "/databricks-datasets/bikeSharing/README.md"
data = sc.textFile(path) # use the sc context to read in a text file
data.count()

# COMMAND ----------

# DBTITLE 1,Take a look at the first line
data.first()

# COMMAND ----------

# DBTITLE 1,Show the top 20 lines
data.take(20)

# COMMAND ----------

# read in file from above
logFile = path 

# cache the data
logData = sc.textFile(logFile).cache()

# get number of times "bike" shows up
# use lambda function and lower() to convert the line to lowercase
# use count to figure out how many times this is true
numBikes = logData.filter(lambda s: 'bike' in s.lower()).count()

# show results
print("Lines with 'bike': %i" % (numBikes))

# COMMAND ----------

# DBTITLE 1,Find a Directory with CSVs
# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/csv/datasets/

# COMMAND ----------

# DBTITLE 1,Read in Directory of Files with wholeTextFiles()
# read in directory looking for anything ending in .csv
path = "/databricks-datasets/Rdatasets/data-001/csv/datasets/*.csv"

# use wholeTextFiles to get each file listed separately with {filename, content}
files = sc.wholeTextFiles(path) 

# count how many files there are
files.count()

# COMMAND ----------

# DBTITLE 1,Convert List of Files to DataFrame
# use toDF to convert object to data frame with column names
filenames = files.toDF(['name', 'data'])

# show entire DataFrame
display(filenames)

# COMMAND ----------

# DBTITLE 1,Show only the names using select()
display(filenames.select('name'))

# COMMAND ----------

