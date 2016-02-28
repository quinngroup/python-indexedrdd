"""SimpleApp.py"""
from itertools import groupby
from pyspark import SparkContext
import numpy as np

sc = SparkContext("local", "Simple App")

class MultiputZipper:
    
    def __init__(self, z = lambda (K, U) : V, f = lambda (K, V, U) : V):
    	self.z=z
    	self.f=f

    def apply(thisIter, otherIter):
    	print("in apply of multiput")		
      #thisPart = thisIter.next()
      #Iterator(thisPart.multiput(otherIter, z, f))

def putInIndex(rdd2,elems):
	return put1(elems)

def put1(elems): 
	return put2(elems,lambda id, a: a,lambda id, a, b: b)

def put2(elems, z = lambda K, U : V, f = lambda K, V, U : V):
	updates = sc.parallelize(elems).partitionBy(5)
	print(updates.take(1))
	print("Currying sample!!!!!")
	f1(1)(2)(3,4,5)
	print("Currying sample!!!!!")
	zipPartitionsWithOther(updates)(MultiputZipper(z,f))

def f1(a):
    def g1(b):
        def h1(c, d, e):
            print(a, b, c, d, e)
        return h1
    return g1
  
def zipPartitionsWithOther(other):
	def f2(f):
		print("in f2")
	return f2
	print("Returned from f2")
	partitioned = other.partitionBy(5)
	newPartitionsRDD = partitionsRDD.zipPartitions(partitioned, true)(f)
	#new IndexedRDD(newPartitionsRDD)
   # newPartitionsRDD = partitionsRDD.zipPartitions(partitioned, true)(f)
   # return newPartitionsRDD
 
"""def OtherZipPartitionsFunction[V2, V3] = Function2[Iterator[IndexedRDDPartition[K, V]], Iterator[(K, V2)], Iterator[IndexedRDDPartition[K, V3]]]"""
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
	rdd2=apply(rdd1).cache()
	
	print("GET Output *******************************************************")
	list1=[(0,12)]
	print(getFromIndex(rdd2,list1))
	print("GET Output *******************************************************")


	print("PUT Output *******************************************************")
	list1=[(1,10873)]
	rdd3=putInIndex(rdd2,list1)
	#print(rdd.take(20))
	#print("PUT Output *******************************************************")
	

if __name__ == "__main__":
	main()