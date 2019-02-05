from pyspark import SparkConf, SparkContext
import sys
import operator
import string

inputs=sys.argv[1]
output=sys.argv[2]

conf=SparkConf().setAppName('wikipedia popular')
sc=SparkContext(conf=conf)

assert sys.version_info >=(3,5)
assert sys.version>='2.3'


def words_once(line):
	q=tuple(line.split())
	yield(q)

def filter_record(entire):
	if(entire[1]=='en' and entire[2]!='Main_Page' and not entire[2].startswith('Special:')):
		return entire

def convert_int(line):
		var=list(line)
		var[3]=int(var[3])
		return var


def get_key(kv):
	return kv[0]


def tab_separated(kv):
	return "%s\t%s" % (kv[0], kv[1])



text=sc.textFile(inputs)
words=text.flatMap(words_once)

word1=words.filter(filter_record)


filtered_record=word1.map(convert_int).map(lambda x : (x[0],(x[3],x[2])))
max_count=filtered_record.reduceByKey(max)


outdata = max_count.sortBy(get_key).map(tab_separated)

outdata.saveAsTextFile(output)
