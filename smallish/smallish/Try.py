"""SimpleApp.py"""
from itertools import groupby
from pyspark import SparkContext
import numpy as np

sc = SparkContext("local", "Simple App")

def nonNegativeMod(x, mod):
    rawMod = x % mod
    rawMod = rawMod + (mod if (rawMod < 0) else 0)
    return rawMod

def getPartition(key):
	return nonNegativeMod(hash(key),5)

"""def groupByExample():
	things = [("animal", "bear"), ("animal", "duck"), ("plant", "cactus"), ("vehicle", "speed boat"), ("vehicle", "school bus")]
	for key, group in groupby(things, lambda x: x[0]):
			print "%s : %s." % ( key, group)"""
    
def partitionFunc(d):
	return d
  
def getFromIndex(rdd2,keyList):
	ksByPartition=[]
	for k,v in keyList:
		ksByPartition=getPartition(v)

	print(ksByPartition)
	partitions = ksByPartition
	results = sc.runJob(rdd2, partitionFunc, [2], True)
	
	allResults = []
	for k,v in keyList:
		allResults.append(dict(results).get(v))

	return allResults
	

def makeMap(index,kv):
	mapObject = ((k,v) for (k,v) in kv)
	return (mapObject)

def apply(elems):
	return updatable(elems)

def updatable(elems): 
	return updatable(elems,lambda id, a: a,lambda id, a, b: b)

def updatable(elems, z = lambda K, U : V, f = lambda K, V, U : V):
	#elemsPartitioned=elems.partitionBy(elems.count())
	elemsPartitioned=elems.partitionBy(5)
	elemsPartitioned.collect()
	partitions = elemsPartitioned.mapPartitionsWithIndex((makeMap),True)
	return (partitions)
    
def main():

	logFile = "README.md"  # Should be some file on your system
	logData = sc.textFile(logFile).cache()

	numAs = logData.filter(lambda s: 'a' in s).count()
	numBs = logData.filter(lambda s: 'b' in s).count()

	
	#Create an RDD of key-value pairs with Long keys.
	rdd1 = sc.parallelize(range(50)).map(lambda x: (x, x*x))
	rdd2=apply(rdd1)
	
	print("GET Output *******************************************************")
	list1=[(0,12)]
	print(getFromIndex(rdd2,list1))
	print("GET Output *******************************************************")
	

if __name__ == "__main__":
	main()