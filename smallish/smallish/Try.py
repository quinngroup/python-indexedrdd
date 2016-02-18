"""SimpleApp.py"""
from itertools import groupby
from pyspark import SparkContext


def makeMap(item):
	mapObject = ((k,v) for (k,v) in item)
	return (mapObject)

def apply(elems):
	return updatable(elems)

def updatable(elems): 
	return updatable(elems,lambda id, a: a,lambda id, a, b: b)

def updatable(elems, z = lambda K, U : V, f = lambda K, V, U : V):
	elemsPartitioned=elems.partitionBy(2)
	partitions = elemsPartitioned.mapPartitions(makeMap)
	return (partitions)
    
def main():

	logFile = "README.md"  # Should be some file on your system
	sc = SparkContext("local", "Simple App")
	logData = sc.textFile(logFile).cache()

	numAs = logData.filter(lambda s: 'a' in s).count()
	numBs = logData.filter(lambda s: 'b' in s).count()

	
	#Create an RDD of key-value pairs with Long keys.
	rdd1 = sc.parallelize(range(50)).map(lambda x: (x, x + 1))
	rdd2=apply(rdd1)
	print("Elements Printed Here*******************************************************")
	print(rdd2.take(5))
	print("Elements Print Ends*******************************************************")

if __name__ == "__main__":
	main()