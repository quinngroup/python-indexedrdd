
from itertools import groupby
from pyspark import SparkContext
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, PairDeserializer, \
    PickleSerializer, pack_long, AutoBatchedSerializer 
import numpy as np




class IndexedRDD(RDD):
 
#----------------------- Intialization Methods ---------------------------------
  #def __new__(self, rddObj):
  #self.indexedRDD = tempRDD
  
  def __init__(self,rddObj):
    self.indexedRDD = rddObj
    super(IndexedRDD, self).__init__(self.indexedRDD._jrdd, self.indexedRDD.ctx)
    

#------------------------ Functionalities ---------------------------------------

  def getFromIndex(self,keyList):
    partitions=[]
    for k,v in keyList:
        partitions.append(self.getPartition(v))

    results = self.indexedRDD.ctx.runJob(self, IndexedRDD.getPartitionFunc(keyList), partitions, True)
    return results  

  def putInIndex(self,other):
    otherKV = IndexedRDD.updatable(other) 
    results = self.indexedRDD.union(otherKV)
    return IndexedRDD(results)

  def deleteFromIndex(self,keyList):
    delObj = self.indexedRDD.ctx.runJob(self, IndexedRDD.delFromPartitionFunc(keyList))
    results = self.indexedRDD.ctx.parallelize((key,value) for (key,value) in delObj)
    return IndexedRDD(IndexedRDD.updatable(results))

  def filter(self,pred):   
    return self.mapIndexedRDDPartitions(pred)
  
  def mapIndexedRDDPartitions(self,f):
    newPartitionsRDD = self.indexedRDD.mapPartitions(lambda d: filter(f,d), True)
    return IndexedRDD(newPartitionsRDD)
  
  def innerJoin(self,other,f): 
    rddX=self.indexedRDD.join(other)
    newPartitionsRDD = rddX.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
  def leftJoin(self,other,f): 
    rddX=self.indexedRDD.leftOuterJoin(other)
    newPartitionsRDD = rddX.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
  def fullOuterJoin(self,other,f): 
    rddX=self.indexedRDD.fullOuterJoin(other)
    newPartitionsRDD = rddX.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
#------------------------ Static Methods ---------------------------------------

  @staticmethod
  def updatable(rddObj): 
    return IndexedRDD.updatable(self,lambda id, a: a,lambda id, a, b: b)

  @staticmethod  
  def updatable(rddObj, z = lambda K, U : V, f = lambda K, V, U : V):
    elemsPartitioned = rddObj.partitionBy(2)
    partitions = elemsPartitioned.mapPartitionsWithIndex((IndexedRDD.makeMap),True)
    return (partitions)

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
  def getPartitionFunc(keyList):
    def innerFunc(d):
      for k,v in keyList:
        d1 = {(value) for (key, value) in d if key == v}
      return (d1)
    return innerFunc

  @staticmethod
  def delFromPartitionFunc(keyList):
    def innerFunc(d):
      for k,v in keyList:
        d = {(key, value) for (key, value) in d if key != v}
      return (d)
    return innerFunc

  @staticmethod
  def filterOnPartitionFunc(f):
    def innerFunc(d):
      d = {f(i) for (i) in d }
      return d
    return innerFunc


    
  