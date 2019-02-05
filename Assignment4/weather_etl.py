import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary


def main(inputs, output):
	observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])

	weather = spark.read.csv(inputs, schema=observation_schema)
	correct_data=weather.filter(weather.qflag.isNull()) 
	correct_data1=correct_data.filter(correct_data.station.startswith('CA')) 
	correct_data2=correct_data1.filter(correct_data1.observation==('TMAX'))
	etl_data=correct_data2.select("station","date",(correct_data2.value.cast("Int")/10).alias("tmax"))
	etl_data.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
