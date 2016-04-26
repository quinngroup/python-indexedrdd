"""
Class that represents the underlying structure 
i.e Dictionaries used for indexing the contents of RDD
"""

class DictPartition:

  #Initializing DictPartition which stores elements of RDD as {k:v}
  def __init__(self,partitionElements):
    dictionary=dict()
    for key,value in partitionElements:
  	 dictionary[key]=value
  	
    self.dictionary = dictionary

  #Iterator returning all the elements of self.dictionary as [(k1,v1),(k2,v2)] pairs
  def __iter__(self):
    return self.dictionary.iteritems()


# --------------------------- Dictionary methods to get/put/delete element ---------------------

  #Function to get value of key from the dictionary
  def get(self,key):
    if self.dictionary.has_key(key):
      return self.dictionary[key]

  #Function to put (key,value) pair in the dictionary
  def put(self,keyList):
    if self.dictionary.has_key(keyList[0]):
			del self.dictionary[keyList[0]]

    self.dictionary[keyList[0]]=keyList[1]	
    return (self)
		
  #Function to delete (key,value) pair from the dictionary      
  def delete(self,key):
    if self.dictionary.has_key(key):
	    del self.dictionary[key]
    return (self)
		