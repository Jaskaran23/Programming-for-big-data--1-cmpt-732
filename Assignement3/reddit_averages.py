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
	




def main(inputs, output):
	text=sc.textFile(inputs).map(json.loads)
	pairs=text.flatMap(key_valuepair)
	add_pair=pairs.reduceByKey(adding_pairs)
	avg_pair=add_pair.map(dividing_pairs)
	outdata=avg_pair.map(json.dumps)
	outdata.saveAsTextFile(output)



if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
