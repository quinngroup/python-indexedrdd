
from itertools import groupby
from pyspark import SparkContext
import numpy as np

class IndexedRDD(object):


  def __init__(self,elements):
    self.updatable(elements)

  def updatable(self,elements): 
    return self.updatable(elements,lambda id, a: a,lambda id, a, b: b)

  def updatable(self,elements, z = lambda K, U : V, f = lambda K, V, U : V):
    #elemsPartitioned=elems.partitionBy(elems.count())
    elemsPartitioned=elements.partitionBy(2)
    partitions = elemsPartitioned.mapPartitionsWithIndex((self.makeMap),True)
    return (partitions)

  def makeMap(index,kv):
    mapObject = ((k,v) for (k,v) in kv)
    return (mapObject)

  def getFromIndex(self,keyList):
    ksByPartition=[]
    partitions=[]
    for k,v in keyList:
        ksByPartition=self.getPartition(v)

    #print(ksByPartition)
    partitions = ksByPartition
    results = sc.runJob(self, self.partitionFunc, [1], True)
  
    allResults = []
    for k,v in keyList:
      allResults.append(dict(results).get(v))
    

    return allResults

  def nonNegativeMod(x, mod):
    rawMod = x % mod
    rawMod = rawMod + (mod if (rawMod < 0) else 0)
    return rawMod

  def getPartition(key):
    return nonNegativeMod(hash(key),2)  
  
  def partitionFunc(d):
    return d
