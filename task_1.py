from time import time
import pyspark
from pyspark.sql import SparkSession
import glob
import time

start_time = time.time()
# config = pyspark.SparkConf().setAll([('spark.executor.memory', '1g'), ('spark.driver.memory','1g')])
spark = SparkSession.builder.appName('kumparan_task3').config('spark.executor.memory', '1g').getOrCreate()
sc = spark.sparkContext

with open("task_1_result.txt", "w+") as task_1:
    last_balance = 0
    for af in sorted([i for i in glob.glob(f"data/*.csv")]):
        df = spark.read.option('header','true').csv(af).select('account_balance')
        last_balance = df.agg({'account_balance': 'sum'}).first()[0] + last_balance
    task_1.write(str(int(last_balance)))

end_time = time.time()
print('Total Time :'+ str(end_time - start_time))
