
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
    rdd1 = sc.parallelize(range(8)).map(lambda x: (x, x*x))
    return IndexedRDD.updatable(rdd1)

  @staticmethod
  def initialize_method2():
    rdd2 = sc.parallelize(range(4,8)).map(lambda x:(x,x*x*x))
    return IndexedRDD.updatable(rdd2)

  @staticmethod
  def initialize_method3(elements):
    rddObj = sc.parallelize((key,value) for (key,value) in elements)
    return IndexedRDD.updatable(rddObj)
 
#-----------------------------------------------------------------------------
  def __init__(self, elements):
    self.ctx = elements.ctx
    self.elements = elements
    self._jrdd_deserializer = self.ctx.serializer
    self._jrdd = elements._jrdd
    
  
  @staticmethod
  def updatable(elements): 
    return IndexedRDD.updatable(self,lambda id, a: a,lambda id, a, b: b)

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
  def partitionFunc(keyList):
    def innerFunc(d):
      for k,v in keyList:
        d1 = {(value) for (key, value) in d if key == v}
      return (d1)
    return innerFunc


  @staticmethod
  def partitionFunc2(keyList):
    def innerFunc(d):
      for k,v in keyList:
        d = {(key, value) for (key, value) in d if key != v}
      return (d)
    return innerFunc

  @staticmethod
  def partitionFuncX(f):
    def innerFunc(d):
      d = {f(i) for (i) in d }
      return d
    return innerFunc


#------------------------ Functionalities ---------------------------------------

  def getFromIndex(self,keyList):
    ksByPartition=[]
    partitions=[]
    for k,v in keyList:
        ksByPartition=self.getPartition(v)

    partitions = ksByPartition
    results = self.ctx.runJob(self, IndexedRDD.partitionFunc(keyList), [1], True)
    return results  

  def putInIndex(self,rddY):
    rddZ1 = IndexedRDD.updatable(rddY) 
    rddZ2 = self.elements.union(rddZ1)
    return IndexedRDD(rddZ2)

  def deleteFromIndex(self,keyList):
    results = sc.runJob(self, IndexedRDD.partitionFunc2(keyList))
    rddObj = IndexedRDD.initialize_method3(results)
    return IndexedRDD(rddObj)

  def filter(self,pred):   
    return self.mapIndexedRDDPartitions(pred)
  
  def mapIndexedRDDPartitions(self,f):
    newPartitionsRDD = self.elements.mapPartitions(lambda d: filter(f,d), True)
    return IndexedRDD(newPartitionsRDD)
  
  def innerJoin(self,other,f): 
    rddX=self.elements.join(other)
    newPartitionsRDD = rddX.mapPartitions(IndexedRDD.partitionFuncX(f), True)
    return IndexedRDD(newPartitionsRDD)
  
#------------------------ Private Custom Classes ---------------------------------------

    
  