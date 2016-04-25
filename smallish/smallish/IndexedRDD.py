
from itertools import groupby
from pyspark import SparkContext
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, PairDeserializer, \
    PickleSerializer, pack_long, AutoBatchedSerializer 
import numpy as np
import itertools
import time
from blist import *
from MyHashMap import MyHashMap
from MyList import MyList


class IndexedRDD(RDD):
 
#----------------------- Intialization Methods ---------------------------------
  #def __new__(self, rddObj):
  #self.indexedRDD = tempRDD
  partitionMod = 1
  
  def __init__(self,rddObj):
    self.indexedRDD = rddObj
    #self.partitioner = rddObj.partitioner
    #self.partitionCount = rddObj.getNumPartitions()
    super(IndexedRDD, self).__init__(self.indexedRDD._jrdd, self.indexedRDD.ctx)
    

#------------------------ Functionalities ---------------------------------------

  def getFromIndex(self,key):
    results = self.indexedRDD.ctx.runJob(self,IndexedRDD.getPartitionFunc(key),[0],True)
    return (results)

  def putInIndex(self,keyList):
    partitionID = self.indexedRDD.partitioner(keyList[0])
    results = self.indexedRDD.mapPartitionsWithIndex(IndexedRDD.putPartitionFunc(partitionID,keyList),True)
    return IndexedRDD(results)

  def deleteFromIndex(self,key):
    partitionID = self.indexedRDD.partitioner(key)
    results = self.indexedRDD.mapPartitionsWithIndex(IndexedRDD.delFromPartitionFunc(partitionID,key),True)
    return IndexedRDD(results)

  def filter(self,pred):   
    return self.mapIndexedRDDPartitions(pred)
  
  def mapIndexedRDDPartitions(self,f):
    newPartitionsRDD = self.indexedRDD.mapPartitions(lambda d: filter(f,d), True)
    return IndexedRDD(IndexedRDD.updatable(newPartitionsRDD))
  
  def innerJoin(self,other,f): 
    if (self.getNumPartitions()==other.getNumPartitions()) :
      rddX=self.indexedRDD.join(other)
    else:
      otherX=other.partitionBy(self.getNumPartitions())
      rddX=self.indexedRDD.join(otherX)

    r2 = IndexedRDD.updatable(rddX)
    newPartitionsRDD = r2.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
  def leftJoin(self,other,f): 
    
    if (self.getNumPartitions()==other.getNumPartitions()) :
      rddX=self.indexedRDD.leftOuterJoin(other)
    else:
      otherX=other.partitionBy(self.getNumPartitions())
      rddX=self.indexedRDD.leftOuterJoin(otherX)

    r2 = IndexedRDD.updatable(rddX)
    newPartitionsRDD = r2.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
  def fullOuterJoin(self,other,f): 
    if (self.getNumPartitions()==other.getNumPartitions()) :
      rddX=self.indexedRDD.fullOuterJoin(other)
    else:
      otherX=other.partitionBy(self.getNumPartitions())
      rddX=self.indexedRDD.fullOuterJoin(otherX)

    r2 = IndexedRDD.updatable(rddX)  
    newPartitionsRDD = r2.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
#------------------------ Static Methods ---------------------------------------

  @staticmethod
  def updatable(rddObj): 
    return IndexedRDD.updatable(self,lambda id, a: a,lambda id, a, b: b)

  @staticmethod  
  def updatable(rddObj, z = lambda K, U : V, f = lambda K, V, U : V):
     if rddObj.partitioner is not None:
          elemsPartitioned = rddObj
     else:
          elemsPartitioned = rddObj.partitionBy(rddObj.getNumPartitions())
     
     #IndexedRDD.partitionMod = elemsPartitioned.getNumPartitions()
     partitions = elemsPartitioned.mapPartitions((lambda iter1 : MyList(MyHashMap(iter1))),True) 
     return (partitions)

  @staticmethod
  def createIter(object):
    list1=[]
    list1.append(object)
    return list1

  @staticmethod   
  def makeMap(index,kv):
    
    mapObject = ({k:v} for k,v in kv)
    #x=iter(mapObject)
    #lst.append(mapObject)
    print(mapObject)
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
    def innerFunc(partIter):
      z = MyList(partIter)
      y = MyHashMap(z.x[0])
      d1 = y.x[keyList]
      return [d1]
    return innerFunc

  @staticmethod
  def putPartitionFunc(partitionID,keyList):
    def innerFunc(index,d):
      if(partitionID==index):
        y=MyHashMap(d.x[0])
        y=y.put(keyList)
        d.x[0]=y
        
      return(d)     
    return innerFunc


  @staticmethod
  def delFromPartitionFunc(partitionID,key):
    def innerFunc(index,d):
       if(partitionID==index):
        y=MyHashMap(d.x[0])
        y=y.delete(key)
        d.x[0]=y

       return(d)     
    return innerFunc

  @staticmethod
  def filterOnPartitionFunc(f):
    def innerFunc(d):
      d = {f(i) for (i) in d }
      return d
    return innerFunc


def main():

  sc = SparkContext("local", "Simple App")
  
  print("Class initialization *******************************************************")
  rdd_1 = sc.parallelize(range(100000)).map(lambda x: (x, x*x))
  rdd_12 = rdd_1.partitionBy(5)
  

  #print(time.time() ,  " Time before creating list of dictionaries")
  rdd_11 = IndexedRDD.updatable(rdd_12)
  #print(time.time() ,  " Time after creating list of dictionaries")
  print("Class initialization *******************************************************")

  rdd_2 = IndexedRDD(rdd_11)
  
  #print("Before call to collect()")
  print(rdd_2.take(1))
  #print("After call to collect()")
  #print("Before call to take()")
  #print(rdd_2.take(1))
  #print("After call to take")
 

  print("Before GET Output *******************************************************")
  print(time.time() ,  " Time before get")
  print(rdd_2.getFromIndex(15))
  print(time.time() ,  " Time after get")
  print("After GET Output *******************************************************")
  
    
  

  
  """print("PUT Output *******************************************************")
  list1=(25,123)
  rdd_4 = rdd_2.putInIndex(list1).cache()
  print (rdd_4.collect())
  print (rdd_4.getNumPartitions())
  print("PUT Output *******************************************************")"""

  """print(rdd_4.take(2))
  print(rdd_4.getNumPartitions())
  #print(rdd_4.getFromIndex(6))
  print("PUT Output *******************************************************")"""
  

  """print("DEL Output *******************************************************")
  rdd_6 = rdd_2.deleteFromIndex(7)
  #print(rdd_6.collect())
  print(rdd_6.getNumPartitions())
  #print(rdd_6.getFromIndex(7))
  print(rdd_6.collect())
  print("DEL Output *******************************************************")""" 

  """print("Filter Output *******************************************************")
  #print(rdd_6.collect())
  rdd_7 = rdd_2.filter(lambda (x):(x[0]%2==0))
  print(rdd_7.collect())
  print("Filter Output *******************************************************")"""

  """ print("Join Output *******************************************************")
  rdd_7 = sc.parallelize(range(10,21)).map(lambda x:(x,x*x*x))
  rdd_8 = IndexedRDD.updatable(rdd_7.partitionBy(5))
  rdd_81 = IndexedRDD(rdd_8)
  
  print(type(rdd_81))
  
  rdd_9 = rdd_2.leftJoin(rdd_81,lambda (id,(a,b)):(id,(a,b)))
  print(rdd_9.collect())
  print(rdd_9.getNumPartitions())
  print("Join Output *******************************************************")
 # print(rdd_9.getFromIndex(5))"""
  

     

if __name__ == "__main__":
  main()
    
  