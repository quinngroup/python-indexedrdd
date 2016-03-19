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


"""	rdd3 = sc.parallelize(range(6,7)).map(lambda x:(x,x*x))
	rdd4 = putInIndex(rdd2,rdd3).cache()
	
	print("GET Output *******************************************************")
	print(rdd4.collect())
	print(rdd4.getNumPartitions())
	print(getFromIndex(rdd4,[(0,5)]))
	print("GET Output *******************************************************")"""

	

if __name__ == "__main__":
	main()