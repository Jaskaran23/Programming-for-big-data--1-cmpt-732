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
    weather.createOrReplaceTempView("weather")
    correct_data = spark.sql("select * from weather where qflag IS NULL ")
    correct_data.createOrReplaceTempView("correct_data")
    temp_max = spark.sql("select station,date,value as tmax from correct_data where observation like 'TMAX'")
    temp_max.createOrReplaceTempView("temp_max")
    temp_min = spark.sql("select station as min_station,date as min_date,value as tmin from correct_data where observation like 'TMIN'")
    temp_min.createOrReplaceTempView("temp_min")
    temp_data_join = spark.sql("select x.station,x.date,x.tmax,y.min_station,y.min_date,y.tmin from temp_max x  INNER JOIN temp_min y on ((x.station = y.min_station )AND(x.date =y.min_date))")
    temp_data_join.createOrReplaceTempView("temp_data_join")
    temp_data_join1 = spark.sql("select *, (tmax-tmin)/10 as range from temp_data_join")
    temp_data_join1.createOrReplaceTempView("temp_data_join1")
    data_max = spark.sql("select date, max(range) as max_range from temp_data_join1 GROUP BY date")
    data_max.createOrReplaceTempView("data_max")
    data_max1 = spark.sql("select x.date,x.station,y.max_range from temp_data_join1 x INNER JOIN data_max y on ((y.max_range = x.range) AND (y.date = x.date)) ORDER BY x.date,x.station")
    data_max1.createOrReplaceTempView("data_max1")
    data_max1.write.csv(output, mode='overwrite')
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
