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
    correct_data = weather.filter(weather.qflag.isNull()) 
    temp_max = correct_data.filter(correct_data.observation==('TMAX'))
    temp_max_data = temp_max.select("station","date",(temp_max.value.alias("tmax")))
    temp_min = correct_data.filter(correct_data.observation==('TMIN'))
    temp_min_data = temp_min.select(temp_min.station.alias('min_station'),temp_min.date.alias('min_date'),(temp_min.value.alias("tmin")))
    temp_data_join = temp_max_data.join(temp_min_data, (temp_min_data.min_station == temp_max_data.station) & (temp_min_data.min_date == temp_max_data.date)).cache()
    temp_data_join = temp_data_join.withColumn('range',(temp_data_join.tmax - temp_data_join.tmin)/10)
    final_data = temp_data_join.groupBy('date',).agg(functions.max('range')).sort('date')
    final_station = final_data.join(temp_data_join,(final_data['max(range)'] == temp_data_join.range) & (final_data.date == temp_data_join.date))
    final_station1 = final_station.select('min_date','station','max(range)').sort('min_date','station')
    #final_station1.show()
    final_station1.write.csv(output, mode='overwrite')
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
