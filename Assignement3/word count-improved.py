from pyspark import SparkConf, SparkContext
import sys
import re,string
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def words_once(line):
	wordsep=re.compile(r'[%s\s]+' % re.escape(string.punctuation))
	for w in wordsep.split(line):
		yield(w.lower(),1)

def get_key(kv):
	return kv[0]

def output_format(kv):
	k, v = kv
	return '%s %i' % (k,v)

def main(inputs, output):
	text=sc.textFile(inputs).repartition(8)
	words=text.flatMap(words_once)
	word_notempty=words.filter(lambda x: len(x)>0)
	wordcount=word_notempty.reduceByKey(operator.add)
	outdata = wordcount.sortBy(get_key).map(output_format)
	outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Wordcount Improved')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
