from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def key_valuepair(dictvalue):
	reddit_key=dictvalue.get("subreddit")
	score_key=dictvalue.get("score")
	yield (reddit_key,(1,score_key))

def adding_pairs(i,j):
	sumone=0
	scoresum=0
	sumone = i[0]+j[0]
	scoresum = i[1]+j[1]
	return (sumone,scoresum) 

def dividing_pairs(k):
	average = 0.0
	average = (k[1][1]/k[1][0])
	return(k[0],average)

def average_value(z):
	subreddit_value=z[0]
	score_value=z[1][0][0]
	average_val=z[1][1]
	author=z[1][0][1]
	return(subreddit_value,score_value/average_val,author)

	




def main(inputs, output):
	text=sc.textFile(inputs).map(json.loads).cache()
	pairs=text.flatMap(key_valuepair)
	add_pair=pairs.reduceByKey(adding_pairs)
	avg_pair=add_pair.map(dividing_pairs).filter(lambda x: (x[1]>0))

	commentbysub = text.map(lambda c: (c['subreddit'],(c['score'],c['author'])))
	joined_rdd=commentbysub.join(avg_pair)
	result=joined_rdd.map(average_value).sortBy(lambda z: z,False)
	#result=joined_rdd.map(lambda z: (z[0],z[1][0][0]/z[1][1],z[1][0][1])).sortBy(lambda z: z,False)
	

	outdata=result.map(json.dumps)
	outdata.saveAsTextFile(output)



if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
