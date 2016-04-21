
from itertools import groupby
from pyspark import SparkContext
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, PairDeserializer, \
    PickleSerializer, pack_long, AutoBatchedSerializer 
import numpy as np
import itertools




class IndexedRDD(RDD):
 
#----------------------- Intialization Methods ---------------------------------
  #def __new__(self, rddObj):
  #self.indexedRDD = tempRDD
  partitionMod = 1
  
  def __init__(self,rddObj):
    self.indexedRDD = rddObj
    super(IndexedRDD, self).__init__(self.indexedRDD._jrdd, self.indexedRDD.ctx)
    

#------------------------ Functionalities ---------------------------------------

  def getFromIndex(self,key):
    return self.indexedRDD.lookup(key)  

  def putInIndex(self,keyList):
    partitionID=(self.getPartition(keyList[0]))
    results = self.indexedRDD.mapPartitionsWithIndex(IndexedRDD.putPartitionFunc(partitionID,keyList),True)
    r2 = IndexedRDD.updatable(results,IndexedRDD.partitionMod)
    return IndexedRDD(r2)

  def deleteFromIndex(self,key):
    delObj = self.indexedRDD.ctx.runJob(self, IndexedRDD.delFromPartitionFunc(key))
    results=self.indexedRDD.ctx.parallelize(delObj)
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
  def updatable(rddObj,partitions): 
    return IndexedRDD.updatable(self,partitions,lambda id, a: a,lambda id, a, b: b)

  @staticmethod  
  def updatable(rddObj,partitions, z = lambda K, U : V, f = lambda K, V, U : V):
     #if rddObj.getNumPartitions() > 1:
     #     elemsPartitioned = rddObj

     #else:
     elemsPartitioned = rddObj.partitionBy(partitions)
     
     IndexedRDD.partitionMod = partitions
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
    return IndexedRDD.nonNegativeMod(hash(key),IndexedRDD.partitionMod)  
  
  @staticmethod
  def getPartitionFunc(keyList):
    def innerFunc(d):
      d2 = dict((key,value) for key,value in d)
      for k,v in keyList:
        d1 = {d2.has_key(v) and d2[v]}
      return (d1)
    return innerFunc

  @staticmethod
  def putPartitionFunc(partitionID,keyList):
    def innerFunc(index,d):
      d1=[]
      if(partitionID==index):
        d1 = [item for item in itertools.ifilterfalse(lambda (k,v): (k==keyList[0]), d)]
        d1.append((keyList[0],keyList[1]))
        
      else:
        return d

      return(d1)     
    return innerFunc


  @staticmethod
  def delFromPartitionFunc(key):
    def innerFunc(d):
      #for k,v in keyList:
      d = {item for item in itertools.ifilterfalse(lambda (k,v): (k==key), d)}
      return (d)
    return innerFunc

  @staticmethod
  def filterOnPartitionFunc(f):
    def innerFunc(d):
      d = {f(i) for (i) in d }
      return d
    return innerFunc


    
  