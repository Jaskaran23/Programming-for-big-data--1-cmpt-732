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

def output_line(orderkey, price, names):
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)



def main(keyspace, outdir, orderkeys):
    orderkeys = ([int(k) for k in orderkeys])
    df1 = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace).load()
    df2 = df1.filter(df1.orderkey.isin(orderkeys))
    #df2.show()
    df3 = df2.select('orderkey','totalprice','part_names').sort('orderkey')
    #df3.show()
    final_rdd = df3.rdd
    #print (final_rdd.take(1))
    final_rdd1 = final_rdd.map(lambda x: output_line(x[0],x[1],x[2]))
    #print(final_rdd1.take(1))
    final_rdd1.saveAsTextFile(outdir)

   

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)
