import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as f
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
task_4_sum = spark.sql("SELECT SUM(account_balance) AS account_balance FROM tbl_kumparan ")
task_4_sum = task_4_sum.withColumn("account_balance",task_4_sum.account_balance.cast(LongType())).first()[0]

task_4_ratio = spark.sql('''
WITH summary AS(
    SELECT 
        account_id, 
        SUM(account_balance) AS account_balance 
    FROM tbl_kumparan 
    GROUP BY account_id
) SELECT (MAX(account_balance)/SUM(account_balance))*100 as ratio FROM summary
''')
task_4_ratio = task_4_ratio.withColumn("ratio",task_4_ratio.ratio.cast(LongType())).first()[0]
# print(task_4_ratio.)


with open("task_4_result.txt", "w+") as task_1:
    task_1.write(str(int(task_4_sum))+'\n'+str(int(task_4_ratio)))
    
end_time = time.time()
print('Total Time :' +str(end_time - start_time))