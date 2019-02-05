from pyspark import SparkConf, SparkContext
import sys
import operator
import re,string

inputs=sys.argv[1]
output=sys.argv[2]

conf=SparkConf().setAppName('word count')
sc=SparkContext(conf=conf)

assert sys.version_info >=(3,5)
assert sys.version>='2.3'


def words_once(line):
	for w in wordsep.split(line):
		yield(w.lower(),1)

def get_key(kv):
	return kv[0]

def output_format(kv):
	k, v = kv
	return '%s %i' % (k,v)

wordsep=re.compile(r'[%s\s]+' % re.escape(string.punctuation))
text=sc.textFile(inputs)

words=text.flatMap(words_once)
word_notempty=words.filter(lambda x: len(x)>0)

wordcount=word_notempty.reduceByKey(operator.add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
