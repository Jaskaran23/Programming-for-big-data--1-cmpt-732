import sys
import json
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('reddit_dataframe').getOrCreate()
assert spark.version >='2.3'

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
	time_ext = path.split('/')
	time_part = time_ext[-1]
	time_only = time_part.split('-')
	remove_ext = time_only[-1].split('.')
	return (time_only[1]+'-'+remove_ext[0].replace('0000',''))




def main(inputs, output):
	data_schema = types.StructType([
		types.StructField('language', types.StringType(), True),
		types.StructField('title', types.StringType(), True),
    	types.StructField('views', types.IntegerType(), True),
    	types.StructField('bytecount', types.IntegerType(),True),
    	])

	
	wikipedia_data = spark.read.csv(inputs, schema = data_schema, sep = ' ').withColumn('hour', path_to_hour(functions.input_file_name()))
	wiki_data1 = wikipedia_data.filter(wikipedia_data.language==('en'))
	wiki_data2 = wiki_data1.filter(wiki_data1.title!=('Main_Page'))
	wiki_data3 = wiki_data2.filter(~(wiki_data2.title.startswith('Special:')))
	wiki_data4 = wiki_data3.groupby('hour').agg(functions.max(wiki_data3['views'])).cache()
	wiki_data4 = wiki_data4.withColumnRenamed('hour', 'filename1')
	#wiki_data4 = functions.broadcast(wiki_data4)
	wiki_data5 = wiki_data3.join(wiki_data4, (wiki_data3.hour == wiki_data4.filename1) & (wiki_data3.views == wiki_data4['max(views)']))
	wiki_data6 = wiki_data5.select('hour','title','views').sort('hour','title')
	wiki_data6.write.json(output, mode='overwrite')
	#wiki_data6.explain()


if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)
