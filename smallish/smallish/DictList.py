"""
Wrapper class to wrap the objects
of dictionary
"""

class DictList:

	def __init__(self,dictPartitionList):
		self.dictPartitionList=list()
		self.dictPartitionList.append(dictPartitionList)

	#Iterator which invokes the iterator of the enclosing dictionary and returns [(key1,value1),(key2,value2)] pairs	
	def __iter__(self):
		return(self.dictPartitionList[0].__iter__())

	