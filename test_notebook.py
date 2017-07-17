# Databricks notebook source
# MAGIC %md
# MAGIC # Test Notebook to be Run with Codepipeline
# MAGIC 
# MAGIC **Business case:**  
# MAGIC Run as part of Integration testing/Continuous Deployment  
# MAGIC **Problem Statement:**  
# MAGIC  
# MAGIC **Business solution:**  
# MAGIC   
# MAGIC **Technical Solution:**  
# MAGIC 
# MAGIC 
# MAGIC Owner: Kevin  
# MAGIC Runnable: Yes  
# MAGIC Last Tested Spark Version: Spark 2.1

# COMMAND ----------

print("hi")

# COMMAND ----------

# dbutils.widgets.text("source", "mnt/kevin/test-input/", "Source Folder")
# dbutils.widgets.text("target", "mnt/kevin/test-output/", "Target Folder")
# dbutils.widgets.text("run_mode", "prod", "Run Mode")


# COMMAND ----------

# MAGIC %md ####Get Notebook Parameters

# COMMAND ----------

import json
source = dbutils.widgets.get("source")
target = dbutils.widgets.get("target")
mode = dbutils.widgets.get("run_mode")
# mode = 'test'
print("source: " + source)
print("target: " + target)
print("mode: " + mode)

# COMMAND ----------

# MAGIC %md %md ####Ingest Source Files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import BooleanType, StringType

tag_schema = StructType([
  StructField('tag', StringType(), True),
  StructField('version', StringType(), True),
  StructField('custom', BooleanType(), True),
  StructField('abstract', BooleanType(), True),
  StructField('datatype', StringType(), True),
  StructField('iord', StringType(), True),
  StructField('crdr', StringType(), True),
  StructField('tlabel', StringType(), True),
  StructField('doc', StringType(), True)]
 )


# filePath = 's3a://{0}:{1}@databricks-kevin/test-input/tag.txt'.format(AccessKey, SecretKey)
# filePath = 's3a://{0}:{1}@{2}'.format(AccessKey, SecretKey, source)

tag_df = spark.read.option("header","true").option("sep","\t").csv(source)
print 'Tag Schema:'
tag_df.printSchema()
tag_df.show(1)


# COMMAND ----------

# MAGIC %md If running in test mode it will writethe data out to S3 into a test location and provide a pointer to the 

# COMMAND ----------

if mode == 'test':
  sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
  tag_df.coalesce(1).write.mode("overwrite").parquet(target)
  values = {"status": "Test", "test_loc": target}
  dbutils.notebook.exit(json.dumps(values))
  

# COMMAND ----------

sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
tag_df.coalesce(1).write.mode("overwrite").parquet(target)
values = {"status": "Went to wrong spot", "test_loc": target}
dbutils.notebook.exit(json.dumps(values))