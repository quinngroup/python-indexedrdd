
from itertools import groupby
from pyspark import SparkContext
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, PairDeserializer, \
    PickleSerializer, pack_long, AutoBatchedSerializer 
import numpy as np
import itertools
import time
from DictPartition import DictPartition
from DictList import DictList


class IndexedRDD(RDD):
 
#----------------------- Intialization Methods ---------------------------------
  def __init__(self,rddObj):
    updatedRDD = IndexedRDD.updatable(rddObj)
    self.indexedRDD = updatedRDD
    #self.partitioner = rddObj.partitioner
    #self.partitionCount = rddObj.getNumPartitions()
    super(IndexedRDD, self).__init__(self.indexedRDD._jrdd, self.indexedRDD.ctx)


  @staticmethod
  def updatable(rddObj): 
    return IndexedRDD.updatable(lambda id, a: a,lambda id, a, b: b)

  @staticmethod  
  def updatable(rddObj, z = lambda K, U : V, f = lambda K, V, U : V):
     if rddObj.partitioner is not None:
          elemsPartitioned = rddObj
     else:
          elemsPartitioned = rddObj.partitionBy(rddObj.getNumPartitions())
     
     partitions = elemsPartitioned.mapPartitions((lambda elementsIter : DictList(DictPartition(elementsIter))),True) 
     return (partitions)
    

#------------------------ Functionalities ---------------------------------------


#------------------------ Get related functions ---------------------------------
  """ Function to get a value corresponding to the specified key, if any. """
  def get(self,key):
    partitionIDs = []
    partitionIDs.append(self.indexedRDD.partitioner(key))
    results = self.indexedRDD.ctx.runJob(self,IndexedRDD.getElementsFromPartition(key),partitionIDs,True)
    return (results)

  @staticmethod
  def getElementsFromPartition(key):
    def innerFunc(partElements):
      dListObj = DictList(partElements)
      dictPartitionObj = DictPartition(dListObj.dictPartitionList[0])
      return [dictPartitionObj.get(key)]
    return innerFunc


#------------------------ Put related functions ---------------------------------
  """ Unconditionally updates the specified key to have the specified value. Returns a new IndexedRDD
  that reflects the modification."""
  def put(self,keyList):
    partitionID = self.indexedRDD.partitioner(keyList[0])
    results = self.indexedRDD.mapPartitionsWithIndex(IndexedRDD.putElementsInPartition(partitionID,keyList),True)
    return IndexedRDD(results)

  @staticmethod
  def putElementsInPartition(partitionID,keyList):
    def innerFunc(index,partElements):
      if(partitionID==index):
        dictPartitionObj=DictPartition(partElements.dictPartitionList[0])
        dictPartitionObj=dictPartitionObj.put(keyList)
        partElements.dictPartitionList[0]=dictPartitionObj
      
      return(partElements)     
    return innerFunc


#------------------------ Delete related functions ---------------------------------  
  """ Deletes the specified keys. Returns a new IndexedRDD that reflects the deletions """
  def delete(self,key):
    partitionID = self.indexedRDD.partitioner(key)
    results = self.indexedRDD.mapPartitionsWithIndex(IndexedRDD.delFromPartitionFunc(partitionID,key),True)
    return IndexedRDD(results)

  @staticmethod
  def delFromPartitionFunc(partitionID,key):
    def innerFunc(index,partElements):
       if(partitionID==index):
        dictPartitionObj=DictPartition(partElements.dictPartitionList[0])
        dictPartitionObj=dictPartitionObj.delete(key)
        partElements.dictPartitionList[0]=dictPartitionObj

       return(partElements)     
    return innerFunc


#------------------------ Filter related functions ---------------------------------  
  """ Filters the elements of IndexedRDD based on the given predicate """
  def filter(self,predicate):   
    return self.mapIndexedRDDPartitions(predicate)

  def mapIndexedRDDPartitions(self,f):
    newPartitionsRDD = self.indexedRDD.mapPartitions(lambda d: filter(f,d), True)
    return IndexedRDD(IndexedRDD.updatable(newPartitionsRDD))

#------------------------ Join related functions ---------------------------------  
  """ Inner joins `self` with `other`, running `f` on the values of corresponding keys """
  def join(self,other,f): 
    if (self.getNumPartitions()==other.getNumPartitions()) :
      joinedRDD=self.indexedRDD.join(other)
    else:
      otherRDD=other.partitionBy(self.getNumPartitions())
      joinedRDD=self.indexedRDD.join(otherRDD)

    resultRDD = IndexedRDD.updatable(joinedRDD)
    newPartitionsRDD = resultRDD.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
  """ Left outer joins `this` with `other`, running `f` on all values of `this`. """
  def leftJoin(self,other,f): 
    if (self.getNumPartitions()==other.getNumPartitions()) :
      joinedRDD=self.indexedRDD.leftOuterJoin(other)
    else:
      otherRDD=other.partitionBy(self.getNumPartitions())
      joinedRDD=self.indexedRDD.leftOuterJoin(otherRDD)

    resultRDD = IndexedRDD.updatable(joinedRDD)
    newPartitionsRDD = resultRDD.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)
  
  """ Joins `self` with `other`, running `f` on the values of all keys in both sets. Note that for
  efficiency `other` must be an IndexedRDD, not just a pair RDD. """
  def fullOuterJoin(self,other,f): 
    if (self.getNumPartitions()==other.getNumPartitions()) :
      joinedRDD=self.indexedRDD.fullOuterJoin(other)
    else:
      otherRDD=other.partitionBy(self.getNumPartitions())
      joinedRDD=self.indexedRDD.fullOuterJoin(otherRDD)

    resultRDD = IndexedRDD.updatable(joinedRDD)  
    newPartitionsRDD = resultRDD.mapPartitions(IndexedRDD.filterOnPartitionFunc(f), True)
    return IndexedRDD(newPartitionsRDD)

  @staticmethod
  def filterOnPartitionFunc(f):
    def innerFunc(d):
      d = {f(i) for i in d }
      return d
    return innerFunc


def main():

  sc = SparkContext("local", "IndexedRDD App")
  
  print("Class initialization *******************************************************")
  initRdd = sc.parallelize(range(100000)).map(lambda x: (x, x*x))
  partitionedRDD = initRdd.partitionBy(5)
  indexedRDD1 = IndexedRDD(partitionedRDD)
  print(indexedRDD1.take(1))
  print("Class initialization *******************************************************")



  print("Before GET Output *******************************************************")
  print(time.time() ,  " Time before get")
  print(indexedRDD1.get(15))
  print(time.time() ,  " Time after get")
  print("After GET Output *******************************************************")
  
    
  
  print("PUT Output *******************************************************")
  print (indexedRDD1.get(25))
  print(time.time() ,  " Time before put")
  putRDD = indexedRDD1.put((25,123)).cache()
  print(time.time() ,  " Time after put")
  print (putRDD.get(25))
  print("PUT Output *******************************************************")

  

  print("DEL Output *******************************************************")
  print (putRDD.get(7))
  print(time.time() ,  " Time before DELETE")
  delRDD = putRDD.delete(7)
  print(time.time() ,  " Time after DELETE")
  print (delRDD.get(7))
  print("DEL Output *******************************************************") 


  print("Filter Output *******************************************************")
  filterRDD = delRDD.filter(lambda (x):(x[0]%2==0))
  print(filterRDD.take(2))
  print("Filter Output *******************************************************")

  print("Join Output *******************************************************")
  rddX = sc.parallelize(range(10,21)).map(lambda x:(x,x*x*x))
  rddY = IndexedRDD.updatable(rddX.partitionBy(5))
  rddZ = IndexedRDD(rddY)
  print(time.time() ,  " Time before join")
  joinRDD = delRDD.leftJoin(rddZ,lambda (id,(a,b)):(id,(a,b)))
  print(time.time() ,  " Time after join")
  print(joinRDD.take(1))
  print("Join Output *******************************************************")
  

     

if __name__ == "__main__":
  main()
    
  