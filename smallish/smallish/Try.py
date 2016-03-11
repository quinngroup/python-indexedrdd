"""SimpleApp.py"""
from pyspark import SparkContext
from IndexedRDD import IndexedRDD

def main():

	sc = SparkContext("local", "Simple App")

	#Create an RDD of key-value pairs with Long keys.
	rdd1 = sc.parallelize(range(6)).map(lambda x: (x, x*x))
	rdd2 = IndexedRDD(rdd1)
	print("Class initialization *******************************************************")
	print(type(rdd2))
	print("Class initialization *******************************************************")


	"""print("GET1 Output *******************************************************")
	list1=[(0,3)]
	print(getFromIndex(rdd2,list1))
	print("GET1 Output *******************************************************")"""


"""	rdd3 = sc.parallelize(range(6,7)).map(lambda x:(x,x*x))
	rdd4 = putInIndex(rdd2,rdd3).cache()
	
	print("GET Output *******************************************************")
	print(rdd4.collect())
	print(rdd4.getNumPartitions())
	print(getFromIndex(rdd4,[(0,5)]))
	print("GET Output *******************************************************")"""

	

if __name__ == "__main__":
	main()