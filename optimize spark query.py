# Databricks notebook source
# MAGIC %md 
# MAGIC ##1. Use DataFrame/Dataset over RDD

# COMMAND ----------

1. For Spark jobs, prefer using Dataset/DataFrame over RDD as Dataset and DataFrame’s includes several optimization modules to improve the performance of the Spark workloads.
2. Spark RDD is a building block of Spark programming, even when we use DataFrame/Dataset, Spark internally uses RDD to execute operations/queries but the efficient and optimized way by analyzing your query and creating the execution plan thanks to Project Tungsten and Catalyst optimizer.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Why RDD is slower?

# COMMAND ----------

1. Using RDD directly leads to performance issues as Spark doesn’t know how to apply the optimization techniques and RDD serialize and de-serialize the data when it distributes across a cluster (repartition & shuffling).
2. Serialization and de-serialization are very expensive operations for Spark applications or any distributed systems, most of our time is spent only on serialization of data rather than executing the operations hence try to avoid using RDD.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Is DataFrame is Faster?

# COMMAND ----------

1. Since Spark DataFrame maintains the structure of the data and column types (like an RDMS table) it can handle the data better by storing and managing more efficiently.
2. First, using off-heap storage for data in binary format. Second, generating encoder code on the fly to work with this binary format for your specific objects.
3. Since Spark/PySpark DataFrame internally stores data in binary there is no need of Serialization and deserialization data when it distributes across a cluster hence you would see a performance improvement.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Restricted use of Collect() method

# COMMAND ----------

1. PySpark RDD/DataFrame collect() is an action operation that is used to retrieve all the elements of the dataset (from all nodes) to the driver node. We should use the collect() on smaller dataset usually after filter(), group() e.t.c. Retrieving larger datasets results in OutOfMemory error.
 
2. retrieves all elements in a DataFrame as an Array of Row type to the driver node. 
 
3. Note that collect() is an action hence it does not return a DataFrame instead, it returns data in an Array to the driver. Once the data is in an array, you can use python for loop to process it further.

# COMMAND ----------

PySpark RDD/DataFrame collect() is an action operation that is used to retrieve all the elements of the dataset (from all nodes) to the driver node. We should use the collect() on smaller dataset usually after filter(), group() e.t.c. Retrieving larger datasets results in OutOfMemory error.

# COMMAND ----------

Usually, collect() is used to retrieve the action output when you have very small result set and calling collect() on an RDD/DataFrame with a bigger result set causes out of memory as it returns the entire dataset (from all workers) to the driver hence we should avoid calling collect() on a larger dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ##collect vs select

# COMMAND ----------

select() is a transformation that returns a new DataFrame and holds the columns that are selected whereas collect() is an action that returns the entire data set in an Array to the driver.

# COMMAND ----------

# import pyspark
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# dept = [("Finance",10), \
#     ("Marketing",20), \
#     ("Sales",30), \
#     ("IT",40) \
#   ]
# deptColumns = ["dept_name","dept_id"]
# deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)

# dataCollect = deptDF.collect()

# print(dataCollect)

# dataCollect2 = deptDF.select("dept_name").collect()
# print(dataCollect2)

# for row in dataCollect:
#     print(row['dept_name'] + "," +str(row['dept_id']))

# COMMAND ----------

