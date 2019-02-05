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
from pyspark.sql.functions import col
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(keyspace,inkeyspace):

	orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()
	part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
	lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()
	orders_df = orders_df.withColumnRenamed('orderkey','orderkey1')
	orders_df = orders_df.withColumnRenamed('totalprice','totalprice1')
	lineitem_df = lineitem_df.withColumnRenamed('orderkey','line_orderkey')
	order_line_join = orders_df.join(lineitem_df, orders_df.orderkey1 == lineitem_df.line_orderkey)
	#order_line_join = order_line_join.withColumnRenamed(order_line_join.orderkey, 'line_orderkey')
	#order_line_join.show()
	order_df = order_line_join.select('orderkey1','totalprice1','partkey')
	order_df = order_df.withColumnRenamed('partkey','order_partkey')
	#order_df.show()
	line_part_join = order_df.join(part_df, order_df.order_partkey == part_df.partkey)
	#line_part_join.show()
	line_part_df = line_part_join.select('orderkey1',line_part_join['totalprice1'].alias('price'),'name')
	#line_part_df.show()
	final_df = line_part_df.groupby('orderkey1').agg(functions.first(line_part_df['price']).alias('totalprice'),functions.collect_set(line_part_df['name']).alias('part_names')).sort('orderkey1')
	final_df = final_df.withColumnRenamed('orderkey1','orderkey')
	df2 = final_df.join(orders_df, final_df.orderkey == orders_df.orderkey1)
	#df2.show()
	df3 = df2.select('orderkey','custkey','orderstatus','totalprice','orderdate','order_priority','clerk','ship_priority','comment','part_names').sort('orderkey')
	#df3.show()
	df3.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=inkeyspace).save()
	

if __name__ == '__main__':
    keyspace = sys.argv[1]
    inkeyspace = sys.argv[2]
    main(keyspace,inkeyspace)
