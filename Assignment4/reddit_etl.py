from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def key_valuepair(dictvalue):
	reddit_key=dictvalue.get("subreddit")
	score_key=dictvalue.get("score")
	author_key=dictvalue.get("author")
	yield (reddit_key,score_key,author_key)


	def main(inputs, output):
	text=sc.textFile(inputs).map(json.loads)
	pairs=text.flatMap(key_valuepair)
	reddit_char=pairs.filter(lambda x:('e' in x[0]))

	positive_score=reddit_char.filter(lambda x: (x[1]>0))

	negative_score=reddit_char.filter(lambda x: (x[1]<=0))

	positive=positive_score.map(json.dumps).saveAsTextFile(output+'/positive')
	negative=negative_score.map(json.dumps).saveAsTextFile(output+'/negative')





if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit ')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
