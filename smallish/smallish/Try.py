"""SimpleApp.py"""
from itertools import groupby
from pyspark import SparkContext


def groupByExample():
	things = [("animal", "bear"), ("animal", "duck"), ("plant", "cactus"), ("vehicle", "speed boat"), ("vehicle", "school bus")]
	for key, group in groupby(things, lambda x: x[0]):
			print "%s : %s." % ( key, group)
    
def getFromIndex(keyList):
	ksByPartition = {}
	for k, v in groupby(keyList, lambda x: x[1]):
		#For every key in the get request, find its respective partition
		#ksByPartition = ks.groupBy(k => partitioner.get.getPartition(k))
		print "%s : %s." % (k, v)
	

def makeMap(item):
	mapObject = ((k,v) for (k,v) in item)
	return (mapObject)

def apply(elems):
	return updatable(elems)

def updatable(elems): 
	return updatable(elems,lambda id, a: a,lambda id, a, b: b)

def updatable(elems, z = lambda K, U : V, f = lambda K, V, U : V):
	elemsPartitioned=elems.partitionBy(5)
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

	print("Group By Example*******************************************************")
	groupByExample()
	lis1=[(1,1), (2,2), (3,3)]
	getFromIndex(lis1)

if __name__ == "__main__":
	main()