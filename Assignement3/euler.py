from pyspark import SparkConf, SparkContext
import sys,random
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def eulersum(j):
	random.seed()
	sample=int(j)
	total_iterations=0
	for i in range(1,sample):
		sum=0.0
		while (sum < 1):
			sum =sum + random.random()
			total_iterations = total_iterations + 1
	return total_iterations

def main(inputs):
	samples = int(inputs)
	rdd1=sc.parallelize([samples],numSlices=12)
	rdd2=rdd1.map(eulersum).reduce(operator.add)
	print(rdd2/samples)

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)
