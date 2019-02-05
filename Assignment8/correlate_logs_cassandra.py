from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
import os
import gzip
import uuid
import datetime
from math import sqrt

from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext



def main(keyspace, table_name):
    df1 = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).load()
    web_df1 = df1.select('host','bytes','datetime')
    #web_df1.createOrReplaceTempView('web_df1')
    #print(web_df1.printSchema())
    web_df2 = web_df1.groupBy('host').agg(functions.count('host').alias('x'))
    web_df2 = web_df2.withColumnRenamed('host', 'hostname1')
    #web_df2.show()
    web_df3 = web_df1.groupBy('host').agg(functions.sum('bytes').alias('y'))
    #web_df3.show()
    web_df_join = web_df2.join(web_df3, web_df2.hostname1 == web_df3.host)
    #web_df_join.show()
    web_df4 = web_df_join.select('host','x','y').withColumn('one_val',functions.lit(1))
    #web_df4.show()
    web_df5 = web_df4.groupBy('host','x','y','one_val').agg((functions.pow(web_df4.x,2).alias('x2')),functions.pow(web_df4.y,2).alias('y2'))
    web_df5 = web_df5.withColumn('xy',web_df5.x * web_df5.y)
    #web_df5 = web_df4.select('hostname','one_val','x',(web_df4.x ** web_df4.x).alias('x2'),'y',(web_df4.y ** web_df4.y).alias('y2'),(web_df4.x ** web_df4.y).alias('xy'))
    #web_df5.show()
    sum_one = web_df5.groupBy().sum('one_val','x','y','x2','y2','xy').collect()
    #print(sum_one)
    one_val = sum_one[0][0]
    print('sum_1s: ',one_val)
    x = sum_one[0][1]
    print('sum_x: ', x)
    y = sum_one[0][2]
    print('sum_y: ', y)
    x2 = sum_one[0][3]
    print('sum_x2: ' ,x2)
    y2 = sum_one[0][4] 
    print('sum_y2: ', y2)  
    xy = sum_one[0][5]
    print('sum_xy: ',xy)
    r1 = ((one_val * xy) - (x * y))
    #print('r1: ', r1)
    r2 = (sqrt((one_val * x2) - (x ** 2))) * (sqrt((one_val * y2) - (y ** 2)))
    #print('r2: ',r2)
    r = r1 / r2
    print('r: ', r)
    r2 = r ** 2
    print('r^2: ',r2)
    

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table_name = sys.argv[2]
    main(keyspace, table_name)
