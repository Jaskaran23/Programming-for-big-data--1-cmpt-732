import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
from math import sqrt
#from pyspark.sql.functions import explode
from pyspark.sql.functions import split

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(topic):
	messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092').option('subscribe', topic).load()
	values = messages.select(messages['value'].cast('string'))
	#values.printSchema()
	values = values.select(split(values['value'],' ').alias('val'))
	values = values.select(values['val'][0].cast('float').alias('x'),values['val'][1].cast('float').alias('y'),functions.lit(1).alias('n'))
	values = values.withColumn('xy', values['x'] * values['y'])
	values = values.withColumn('x2', values['x'] * values['x'])
	#values = values.groupBy().sum('x','y','xy','x2','n')
	values = values.groupBy().agg(functions.sum('x').alias('x'),functions.sum('y').alias('y'),functions.sum('xy').alias('xy'),functions.sum('x2').alias('x2'),functions.sum('n').alias('n'))
	values = values.withColumn('beta',((values['xy'] - (values['x'] * values['y'] * (1/values['n']))) / (values['x2'] - ((values['x'] ** 2) * (1/values['n'])))))
	values = values.withColumn('alpha',((values['y'] * (1/values['n'])) - (values['beta'] * values['x'] * (1/values['n']))))
	final_df = values.select(values['beta'].alias('slope'),values['alpha'].alias('intercept'))
	stream1 = final_df.writeStream.outputMode('update').format("console").start()
	stream1.awaitTermination(600)

	
	

if __name__ == '__main__':
    topic = sys.argv[1]
    main(topic)
