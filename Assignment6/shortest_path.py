
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext
import re

# add more functions as necessary
def get_value(x):
	q ,r = x.split(":")
	if len(r) == 0:
		return
	d = r.strip().split(" ")
	#r = q.strip().split(" ")
	for dst in d:
		print(q,dst)
		yield(int(q), int(dst))
	

def main(inputs, output,source, destination):
	one_schema = types.StructType([
        types.StructField('source', types.IntegerType(), False),
        types.StructField('destination', types.IntegerType(), False),
        ])

	#two_schema = types.StructType([
		#types.StructField('node',types.IntegerType(),False),
		#types.StructField('source',types.IntegerType(),True),
		#types.StructField('distance',types.IntegerType(),False),
		#])

	text = sc.textFile(inputs + '/links-simple-sorted.txt')
	words = text.flatMap(get_value)
	df_1 = spark.createDataFrame(words, schema = one_schema)
	graph_node = df_1.select('source','destination').cache()
	graph_node.show()
	
	known_paths = spark.createDataFrame([[source,'-1',0]],['node','source','distance'])
	

	for a in range(6):
		joined_df = known_paths.join(graph_node,(graph_node.source == known_paths.node) & (known_paths.distance == a))
		joined_df.show()
		updated_df = joined_df.select(joined_df.destination.alias('node1'),joined_df.node.alias('source1'),((joined_df.distance)+functions.lit(1)).alias('distance1')).cache()
		updated_df.show()
		updated_df1 = updated_df.join(known_paths, known_paths.node == updated_df.node1)
		updated_df1 = updated_df1.select(updated_df1.node1,updated_df1.source1,updated_df1.distance1)
		updated_df1.show()
		updated_df = updated_df.subtract(updated_df1)
		updated_df.show()

		known_paths = known_paths.unionAll( updated_df).cache()
		known_paths.show()
		known_paths.write.csv(output + '/iter-' + str(a), mode = 'overwrite')

		path_len = known_paths.where(known_paths.node == destination).collect()
		if(len(path_len)>0):
			break;

	list_path = [destination]
	found_path = destination
	while(found_path != source):
		row_val = known_paths.where(known_paths.node == found_path).collect()
		found_path = int(row_val[0][1])
		list_path.append(found_path)
	list_path = list(reversed(list_path))
	final_path = sc.parallelize(list_path)
	final_path.saveAsTextFile(output+ "/path")



		#known_paths = known_paths.unionAll(updated_df).show()
	

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    destination = int(sys.argv[4])
    main(inputs, output, source, destination)
  
