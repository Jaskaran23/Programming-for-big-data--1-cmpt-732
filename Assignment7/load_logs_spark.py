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

from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
def match_pattern(web_log):
    val = re.search(line_re, web_log)
    if val:
        y = line_re.split(web_log)
        y[2] = datetime.datetime.strptime(y[2],"%d/%b/%Y:%H:%M:%S")
        y[4] = int(y[4])
        return (y[1],y[2],y[3],y[4])

@functions.udf(returnType=types.StringType())
def generateUUid():
    unique_id = str(uuid.uuid4())
    return (unique_id)

#val generateUUid = udf(() => UUID.randomUUID().toString)

def main(inputs, keyspace, table_name):
	observation_schema = types.StructType([
        types.StructField('host', types.StringType(), False),
        types.StructField('datetime', types.TimestampType(), False),
        types.StructField('path', types.StringType(), False),
        types.StructField('bytes', types.IntegerType(), False),
        ])

    #session = cluster.connect(keyspace)
    #batch = BatchStatement(consistency_level = ConsistencyLevel.ONE)
    #insert_query = session.prepare ('Insert into '+ table_name +' (host,id,bytes,datetime,path) VALUES (?,?,?,?,?)')
    #counter = 0
    #batch_size = 200

	nasa_logs = sc.textFile(inputs).repartition(40)
	nasa_logs1 = nasa_logs.map(match_pattern)
	nasa_logs2 = nasa_logs1.filter(lambda x: x is not None)
	nasa_df = spark.createDataFrame(nasa_logs2, schema = observation_schema).withColumn('id',generateUUid())
	nasa_df1 = nasa_df.select('host','id','bytes','datetime','path')
	nasa_df1.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).save()

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(inputs, keyspace, table_name)
