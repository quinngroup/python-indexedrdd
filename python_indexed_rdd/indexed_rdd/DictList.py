"""A wrapper class"""

class DictList:
	"""
	Wrapper for dictionary objects
	"""

	def __init__(self, dict_partition_list):
		"""
		Appends dictionary items to list
		"""
		self.dict_partition_list = list()
		self.dict_partition_list.append(dict_partition_list)

	def __iter__(self):
		"""
		Invokes the iterator of the enclosing dictionary
		Returns:
			[(key1,value1),(key2,value2)] pairs.
		"""
		return self.dict_partition_list[0].__iter__()
