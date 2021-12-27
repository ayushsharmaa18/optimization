# Databricks notebook source
ACCESSY_KEY_ID = "AKIAJBRYNXGHORDHZB4A"
SECERET_ACCESS_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF" 

mounts_list = [
{'bucket':'databricks-corp-training/sf_open_data/', 'mount_folder':'/mnt/sf_open_data'}
]

for mount_point in mounts_list:
  bucket = mount_point['bucket']
  mount_folder = mount_point['mount_folder']
  try:
    dbutils.fs.ls(mount_folder)
    dbutils.fs.unmount(mount_folder)
  except:
    pass
  finally: #If MOUNT_FOLDER does not exist
    dbutils.fs.mount("s3a://"+ ACCESSY_KEY_ID + ":" + SECERET_ACCESS_KEY + "@" + bucket,mount_folder)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sf_open_data/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sf_open_data/fire_dept_calls_for_service/

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/sf_open_data/fire_dept_calls_for_service/Fire_Department_Calls_for_Service.csv',inferSchema=True,header=True)

# COMMAND ----------

# df1 = df
df.cache()


# COMMAND ----------

# MAGIC %md
# MAGIC ##cached

# COMMAND ----------

df.groupBy("Call Number").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##uncached

# COMMAND ----------

df=df.unpersist()

# COMMAND ----------

df.groupBy("Call Number").count().display()

# COMMAND ----------

