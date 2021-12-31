# Databricks notebook source
# MAGIC %md ## Markdown Content 
# MAGIC You can add lists like so
# MAGIC - item 1
# MAGIC - item 2
# MAGIC   - sub-item 1
# MAGIC   
# MAGIC I often create links also like this
# MAGIC [My Site](http://bensullins.com)
# MAGIC [Another Site](http://databricks.com)
# MAGIC 
# MAGIC More Markdown can be found [here](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)

# COMMAND ----------

# MAGIC %r
# MAGIC # Now let's create a simple list with 10000 integers
# MAGIC # xrange() is more memory efficient so let's use that
# MAGIC 
# MAGIC data = range(1, 10001)

# COMMAND ----------

# now see how big our list is
len(data)

# COMMAND ----------

# So far we've done just basic Python, now let's use Spark
# Start by using 'sc' to tell Spark we want to use the SparkContext
# Then we use parallelize() to create a Dataset and spread it across
# the cluster partitions

ds = sc.parallelize(data, 8)
# more info on parallelize here 
# help(sc.parallelize)

# COMMAND ----------

# show what we have in ds using the collect() action
print(ds.collect()) # we don't need to use "print" here, but it's better for formatting

# COMMAND ----------

