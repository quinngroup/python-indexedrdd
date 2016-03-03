"""SimpleApp.py"""
from itertools import groupby
from pyspark import SparkContext
import numpy as np

sc = SparkContext("local", "Simple App")


def putInIndex(rddX,rddY):
	rddZ1 = apply(rddY)
	rddZ2 = rddX.union(rddZ1)
	return rddZ2

def nonNegativeMod(x, mod):
    rawMod = x % mod
    rawMod = rawMod + (mod if (rawMod < 0) else 0)
    return rawMod

def getPartition(key):
	return nonNegativeMod(hash(key),2)

def partitionFunc(d):
	return d
  
def getFromIndex(rdd2,keyList):
	ksByPartition=[]
	partitions=[]
	for k,v in keyList:
		ksByPartition=getPartition(v)

	#print(ksByPartition)
	partitions = ksByPartition
	results = sc.runJob(rdd2, partitionFunc, [1], True)
	
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
	elemsPartitioned=elems.partitionBy(2)
	elemsPartitioned.collect()
	partitions = elemsPartitioned.mapPartitionsWithIndex((makeMap),True)
	return (partitions)
    
def main():

	logFile = "README.md"  # Should be some file on your system
	logData = sc.textFile(logFile).cache()

	
	#Create an RDD of key-value pairs with Long keys.
	rdd1 = sc.parallelize(range(6)).map(lambda x: (x, x*x))
	rdd2 = apply(rdd1).cache()

	print("GET1 Output *******************************************************")
	list1=[(0,3)]
	print(getFromIndex(rdd2,list1))
	print("GET1 Output *******************************************************")


	rdd3 = sc.parallelize(range(6,8)).map(lambda x:(x,x*x))
	rdd4 = putInIndex(rdd2,rdd3).cache()
	
	print("GET Output *******************************************************")
	print(rdd4.collect())
	print(getFromIndex(rdd4,[(0,7)]))
	print("GET Output *******************************************************")

	

if __name__ == "__main__":
	main()