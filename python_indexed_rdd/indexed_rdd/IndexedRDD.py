"""
Base Class having all the functions
of IndexedRDD
"""
import itertools
import time
import numpy as np
from pyspark import SparkContext
from pyspark.rdd import RDD, PipelinedRDD
from indexed_rdd import DictPartition
from indexed_rdd import DictList


class IndexedRDD(RDD):
	"""
	An RDD of key-value `(K, V)` pairs which pre-indexes the entries
	"""
	"""
	Intialization Methods
	"""
	def __init__(self, rdd_obj):
		updated_rdd = IndexedRDD.updatable(rdd_obj)
		self.indexed_rdd = updated_rdd
		#self.partitioner = rddObj.partitioner
		#self.partitionCount = rddObj.getNumPartitions()
		super(IndexedRDD, self).__init__(self.indexed_rdd._jrdd, self.indexed_rdd.ctx)

	@staticmethod
	def updatable(rdd_obj, z=lambda K, U: V, f=lambda K, V, U: V):
		if rdd_obj.partitioner is not None:
			elems_partitioned = rdd_obj
		else:
			elems_partitioned = rdd_obj.partitionBy(rdd_obj.getNumPartitions())

		partitions = elems_partitioned.mapPartitions((lambda elements_iter: DictList(DictPartition(elements_iter))), True)
		return partitions


	"""
	Functionalities
	"""
	def get(self, key):
		"""
		Function to get a value corresponding to the specified key, if any.
		"""
		partition_ids = []
		partition_ids.append(self.indexed_rdd.partitioner(key))
		results = self.indexed_rdd.ctx.runJob(self, IndexedRDD.get_elements_from_partition(key), partition_ids, True)
		return results

	@staticmethod
	def get_elements_from_partition(key):
		def inner_func(part_elements):
			d_list_obj = DictList(part_elements)
			dict_partition_obj = DictPartition(d_list_obj.dict_partition_list[0])
			return [dict_partition_obj.get(key)]
		return inner_func


	"""
	Put related functions
	"""
	def put(self, key_list):
		"""
		Unconditionally updates the specified key to have the specified value. Returns a new IndexedRDD
		that reflects the modification.
		"""
		partition_id = self.indexed_rdd.partitioner(key_list[0])
		results = self.indexed_rdd.mapPartitionsWithIndex(IndexedRDD.put_elements_in_partition(partition_id, key_list), True)
		return IndexedRDD(results)

	@staticmethod
	def put_elements_in_partition(partition_id, key_list):
		def inner_func(index, part_elements):
			if partition_id == index:
				dict_partition_obj = DictPartition(part_elements.dict_partition_list[0])
				dict_partition_obj = dict_partition_obj.put(key_list)
				part_elements.dict_partition_list[0] = dict_partition_obj
			return part_elements
		return inner_func

	"""
	Delete related functions
	"""
	def delete(self, key):
		"""
		Deletes the specified keys. Returns a new IndexedRDD that reflects the deletions
		"""
		partition_id = self.indexed_rdd.partitioner(key)
		results = self.indexed_rdd.mapPartitionsWithIndex(IndexedRDD.del_from_partition_func(partition_id, key), True)
		return IndexedRDD(results)

	@staticmethod
	def del_from_partition_func(partition_id, key):
		def inner_func(index, part_elements):
			if partition_id == index:
				dict_partition_obj = DictPartition(part_elements.dict_partition_list[0])
				dict_partition_obj = dict_partition_obj.delete(key)
				part_elements.dict_partition_list[0] = dict_partition_obj
			return part_elements
		return inner_func

	"""
	Filter related functions
	"""
	def filter(self, predicate):
		"""
		Filters the elements of IndexedRDD based on the given predicate
		"""
		return self.map_indexed_rdd_partitions(predicate)

	def map_indexed_rdd_partitions(self, f):
		new_partitions_rdd = self.indexed_rdd.mapPartitions(lambda d: filter(f, d), True)
		return IndexedRDD(IndexedRDD.updatable(new_partitions_rdd))

	"""
	Join related functions
	"""
	def join(self, other, f):
		"""
		Inner joins `self` with `other`, running `f` on the values of corresponding keys
		"""
		if self.getNumPartitions() == other.getNumPartitions():
			joined_rdd = self.indexed_rdd.join(other)
		else:
			other_rdd = other.partitionBy(self.getNumPartitions())
			joined_rdd = self.indexed_rdd.join(other_rdd)

		result_rdd = IndexedRDD.updatable(joined_rdd)
		new_partitions_rdd = result_rdd.mapPartitions(IndexedRDD.filter_on_partition_func(f), True)
		return IndexedRDD(new_partitions_rdd)

	def left_join(self, other, f):
		"""
		Left outer joins `this` with `other`, running `f` on all values of `this`.
		"""
		if self.getNumPartitions() == other.getNumPartitions():
			joined_rdd = self.indexed_rdd.leftOuterJoin(other)
		else:
			other_rdd = other.partitionBy(self.getNumPartitions())
			joined_rdd = self.indexed_rdd.leftOuterJoin(other_rdd)
			
		result_rdd = IndexedRDD.updatable(joined_rdd)
		new_partitions_rdd = result_rdd.mapPartitions(IndexedRDD.filter_on_partition_func(f), True)
		return IndexedRDD(new_partitions_rdd)

	def full_outer_join(self, other, f):
		"""
		Joins `self` with `other`, running `f` on the values of all keys in both sets. Note that for
		efficiency `other` must be an IndexedRDD, not just a pair RDD.
		"""
		if self.getNumPartitions() == other.getNumPartitions():
			joined_rdd = self.indexed_rdd.fullOuterJoin(other)
		else:
			other_rdd = other.partitionBy(self.getNumPartitions())
			joined_rdd = self.indexed_rdd.fullOuterJoin(other_rdd)
			
		result_rdd = IndexedRDD.updatable(joined_rdd)
		new_partitions_rdd = result_rdd.mapPartitions(IndexedRDD.filter_on_partition_func(f), True)
		return IndexedRDD(new_partitions_rdd)

	@staticmethod
	def filter_on_partition_func(f):
		def inner_func(d):
			d = {f(i) for i in d}
			return d
		return inner_func
