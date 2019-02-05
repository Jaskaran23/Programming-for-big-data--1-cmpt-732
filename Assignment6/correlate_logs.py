import sys
from math import sqrt
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext

import re
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

# add more functions as necessary
def match_pattern(web_log):
    val = re.search(line_re, web_log)
    if val:
        y = line_re.split(web_log)
        return (y[1],y[2],y[3],int(y[4]))


def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('hostname', types.StringType(), False),
        types.StructField('datetime', types.StringType(), False),
        types.StructField('path', types.StringType(), False),
        types.StructField('bytecount', types.IntegerType(), False),
        ])

    web_logs = sc.textFile(inputs)
    web_logs1 = web_logs.map(match_pattern)
    web_logs2 = web_logs1.filter(lambda x: x is not None)
    #web_logs2.saveAsTextFile(output)
    web_df = spark.createDataFrame(web_logs2, schema = observation_schema)
    web_df1 = web_df.select('hostname','bytecount')
    #web_df1.show()
    #web_df1.createOrReplaceTempView('web_df1')
    #print(web_df1.printSchema())
    web_df2 = web_df1.groupBy('hostname').agg(functions.count('hostname').alias('x'))
    web_df2 = web_df2.withColumnRenamed('hostname', 'hostname1')
    #web_df2.show()
    web_df3 = web_df1.groupBy('hostname').agg(functions.sum('bytecount').alias('y'))
    #web_df3.show()
    web_df_join = web_df2.join(web_df3, web_df2.hostname1 == web_df3.hostname)
    #web_df_join.show()
    web_df4 = web_df_join.select('hostname','x','y').withColumn('one_val',functions.lit(1))
    #web_df4.show()
    web_df5 = web_df4.groupBy('hostname','x','y','one_val').agg((functions.pow(web_df4.x,2).alias('x2')),functions.pow(web_df4.y,2).alias('y2')).cache()
    web_df5 = web_df5.withColumn('xy',web_df5.x * web_df5.y).cache()
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

    #etl_data.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
