
from itertools import groupby
from pyspark import SparkContext
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, PairDeserializer, \
    PickleSerializer, pack_long, AutoBatchedSerializer 
import numpy as np

sc = SparkContext("local", "Simple App")


class IndexedRDD(RDD):

#------------------------ Initialization Methods -----------------------------

  @staticmethod
  def initialize_method():
    rdd1 = sc.parallelize(range(6)).map(lambda x: (x, x*x))
    return IndexedRDD.updatable(rdd1)

  @staticmethod
  def initialize_method2():
    rdd2 = sc.parallelize(range(6,8)).map(lambda x:(x,x*x))
    return IndexedRDD.updatable(rdd2)
    

  def __init__(self, elements):
    self.ctx = elements.ctx
    self.elements = elements
    self._jrdd_deserializer = self.ctx.serializer
    self._jrdd = elements._jrdd
  
  @staticmethod
  def updatable(elements): 
    return IndexedRDD.updatable(elements,lambda id, a: a,lambda id, a, b: b)

  @staticmethod  
  def updatable(elements, z = lambda K, U : V, f = lambda K, V, U : V):
    elemsPartitioned = elements.partitionBy(2)
    partitions = elemsPartitioned.mapPartitionsWithIndex((IndexedRDD.makeMap),True)
    return partitions

#------------------------ Static Methods ---------------------------------------

  @staticmethod   
  def makeMap(index,kv):
    mapObject = ((k,v) for (k,v) in kv)
    return (mapObject)

  @staticmethod  
  def nonNegativeMod(x, mod):
    rawMod = x % mod
    rawMod = rawMod + (mod if (rawMod < 0) else 0)
    return rawMod

  @staticmethod
  def getPartition(key):
    return IndexedRDD.nonNegativeMod(hash(key),2)  
  
  @staticmethod
  def partitionFunc(d):
    return d

#------------------------ Functionalities ---------------------------------------

  def getFromIndex(self,keyList):
    ksByPartition=[]
    partitions=[]
    for k,v in keyList:
        ksByPartition=self.getPartition(v)

    partitions = ksByPartition
    results = sc.runJob(self, IndexedRDD.partitionFunc, [1], True)
  
    allResults = []
    for k,v in keyList:
      allResults.append(dict(results).get(v))
    
    return allResults  

  def putInIndex(self,rddY):
    rddZ1 = IndexedRDD.updatable(rddY)
    rddZ2 = self.elements.union(rddZ1)
    return IndexedRDD(rddZ2)
 