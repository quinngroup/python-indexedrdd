"""SimpleApp.py"""
from pyspark import SparkContext
from IndexedRDD import IndexedRDD

def main():

	sc = SparkContext("local", "Simple App")
	print("Class initialization *******************************************************")
	rdd_1 = sc.parallelize(range(7)).map(lambda x: (x, x*x))
	rdd_11 = IndexedRDD.updatable(rdd_1)
	rdd_2 = IndexedRDD(rdd_11)
	print(type(rdd_2))
	print(rdd_2.collect())
	print(rdd_2.getNumPartitions())
	print("Class initialization *******************************************************")


	print("GET1 Output *******************************************************")
	list1=[(0,3)]
	print(rdd_2.getFromIndex(list1))
	print("GET1 Output *******************************************************")

	
	print("PUT Output *******************************************************")
	list1=[(7,49)]
	rdd_4 = rdd_2.putInIndex(list1).cache()
	print(rdd_4.collect())
	print(rdd_4.getNumPartitions())
	print(rdd_4.getFromIndex([(0,7)]))
	print("PUT Output *******************************************************")


	"""print("DEL Output *******************************************************")
	rdd_5 = rdd_2.deleteFromIndex([(0,8)])
	print(rdd_5.collect())
	print(rdd_5.getNumPartitions())
	print(rdd_5.getFromIndex([(0,8)]))
	print("DEL Output *******************************************************")

	print("Filter Output *******************************************************")
	rdd_6 = rdd_2.filter(lambda (x):(x[0]%2==0))
	print(rdd_6.collect())
	print("Filter Output *******************************************************")

	

	print("Join Output *******************************************************")
	rdd_7 = sc.parallelize(range(4,8)).map(lambda x:(x,x*x*x))
	rdd_8 = IndexedRDD.updatable(rdd_7)
	print("RDD2 *******************************************************")
	print(rdd_2.collect())
	print(rdd_8.collect())
	print("RDD8 *******************************************************")
	rdd_9 = rdd_2.fullOuterJoin(rdd_8,lambda (id,(a,b)):(id,(b,b)))
	print(rdd_9.collect())
	print(rdd_9.getNumPartitions())
	list1=[(0,4)]
	print(rdd_9.getFromIndex(list1))
	print("Join Output *******************************************************")"""


	

if __name__ == "__main__":
	main()