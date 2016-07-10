"""
Class that represents the underlying structure
i.e Dictionaries used for indexing the contents of RDD
"""


class DictPartition:
	"""
	Class that represents the underlying structure
	i.e Dictionaries used for indexing the contents of RDD
	"""
	def __init__(self, partition_elements):
		dictionary = dict()
		for key, value in partition_elements:
			dictionary[key] = value
			self.dictionary = dictionary

	def __iter__(self):
		"""
		Returns:
			All the elements of self.dictionary as [(k1,v1),(k2,v2)] pairs
		"""
		return self.dictionary.iteritems()


	"""
	Dictionary methods to get/put/delete element
	"""
	def get(self, key):
		"""
		Function to get value of key from the dictionary
		Returns:
			value of a key in dictionary
		"""
		if key in self.dictionary:
			return self.dictionary[key]

	def put(self, key_list):
		"""
		Function to put (key,value) pair in the dictionary
		Returns:
			Dictionary object
		"""
		if key_list[0] in self.dictionary:
			del self.dictionary[key_list[0]]
		self.dictionary[key_list[0]] = key_list[1]
		return self

	def delete(self, key):
		"""
		Function to delete (key,value) pair from the dictionary
		Returns:
			Dictionary object
		"""
		if key in self.dictionary:
			del self.dictionary[key]
		return self
