"""SimpleApp.py"""
from pyspark import SparkContext
from IndexedRDD import IndexedRDD

def main():

	rdd1 = IndexedRDD.initialize_method()
	rdd2 = IndexedRDD(rdd1)
	print("Class initialization *******************************************************")
	print(type(rdd2))
	print(rdd2.collect())
	print(rdd2.getNumPartitions())
	print("Class initialization *******************************************************")


	print("GET1 Output *******************************************************")
	list1=[(0,3)]
	print(rdd2.getFromIndex(list1))
	print("GET1 Output *******************************************************")


	rdd3 = IndexedRDD.initialize_method2()
	rdd4 = rdd2.putInIndex(rdd3).cache()
	
	print("GET Output *******************************************************")
	print(rdd4.collect())
	print(rdd4.getNumPartitions())
	print(rdd4.getFromIndex([(0,5)]))
	print("GET Output *******************************************************")


	print("DEL Output *******************************************************")
	rdd5 = rdd4.deleteFromIndex([(0,5)])
	print(rdd5.collect())
	print(rdd5.getNumPartitions())
	print(rdd5.getFromIndex([(0,5)]))
	print("DEL Output *******************************************************")

	

if __name__ == "__main__":
	main()