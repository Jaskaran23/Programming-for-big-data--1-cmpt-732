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
spark = SparkSession.builder.appName('Spark Cassandra example') .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
#spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def output_line(orderkey, price, names):
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace, outdir, orderkeys):
	#customer_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='customer', keyspace=keyspace).load()
	#nation_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='nation', keyspace=keyspace).load()
	orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()
	#partsupp_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='partsupp', keyspace=keyspace).load()
	part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
	#region_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='region', keyspace=keyspace).load()
	#supplier_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='supplier', keyspace=keyspace).load()
	lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()

	#orderkeys = ','.join(k for k in orderkeys)
	orderkeys = ([int(k) for k in orderkeys])

	
	order_key_df = orders_df.filter(orders_df.orderkey.isin(orderkeys))
	#order_key_df.show()
	lineitem_df = lineitem_df.withColumnRenamed('orderkey','line_orderkey')
	order_line_join = order_key_df.join(lineitem_df, order_key_df.orderkey == lineitem_df.line_orderkey)
	#order_line_join = order_line_join.withColumnRenamed(order_line_join.orderkey, 'line_orderkey')
	#order_line_join.show()
	order_df = order_line_join.select('orderkey','totalprice','partkey')
	order_df = order_df.withColumnRenamed('partkey','order_partkey')
	#order_df.show()
	line_part_join = order_df.join(part_df, order_df.order_partkey == part_df.partkey)
	#line_part_join.show()
	line_part_df = line_part_join.select('orderkey',line_part_join['totalprice'].alias('price'),'name')
	#line_part_df.show()
	final_df = line_part_df.groupby('orderkey').agg(functions.first(line_part_df['price']).alias('price'),functions.collect_set(line_part_df['name']).alias('names')).sort('orderkey')
	#final_df.show()
	#final_df.explain()
	final_rdd = final_df.rdd
	#print(final_rdd[0][0])
	#print (final_rdd)
	final_rdd1 = final_rdd.map(lambda x: output_line(x[0],float(x[1]),x[2]))
	#print(final_rdd1.take(1))
	final_rdd1.saveAsTextFile(outdir)
  

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)
