from time import time
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkContext
import glob
import os
import time

start_time = time.time()
configg = pyspark.SparkConf().setAll([('spark.executor.memory', '1g'), ("spark.driver.maxResultSize", "0")])
spark = SparkSession.builder.master("local").appName('kumparan_task3').config(conf=configg).getOrCreate()
sc = spark.sparkContext

schema = StructType() \
    .add("account_id",StringType(),True) \
    .add("account_balance",LongType(),True)

df = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("data/*.csv")

df = df.filter("account_balance != 0")
kumparan_tmpTbl = df.createOrReplaceTempView("tbl_kumparan")
task_3 = spark.sql("SELECT account_id, SUM(account_balance) AS account_balance FROM tbl_kumparan GROUP BY account_id ORDER BY account_balance DESC LIMIT 1000000")
task_3 = task_3.withColumn("account_balance",task_3.account_balance.cast(LongType()))
task_3.toPandas().to_csv('task_3_result.csv', index=False)

end_time = time.time()
print('Total Time :' +str(end_time - start_time))