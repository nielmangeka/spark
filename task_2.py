from time import time
import pyspark
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
import glob
import time

start_time = time.time()
# config = pyspark.SparkConf().setAll([('spark.executor.memory', '1g'), ('spark.driver.memory','1g')])
spark = SparkSession.builder.appName('kumparan_task3').config('spark.executor.memory', '1g').getOrCreate()
sc = spark.sparkContext

df = spark.read.options(header=True).csv("data/*.csv")
df = df.filter("account_balance != 0")

df = df.withColumn("account_balance", df["account_balance"].cast(IntegerType()))
df = df.withColumn("account_id", df.account_id.substr(1,2)).withColumnRenamed("account_id", "account_group")
df = df.groupBy("account_group").sum("account_balance").withColumnRenamed("sum(account_balance)", "sum").sort("account_group")
df.toPandas().to_csv('task_2_result.csv', index=False)

end_time = time.time()
print('Total Time :' +str(end_time - start_time))