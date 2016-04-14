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
	print(rdd_2.getFromIndex(2))
	print("GET1 Output *******************************************************")


	
	
	print("PUT Output *******************************************************")
	list1=(6,123)
	rdd_4 = rdd_2.putInIndex(list1).cache()
	print(rdd_4.collect())
	print(rdd_4.getNumPartitions())

	print("PUT Output *******************************************************")
	list1=(7,49)
	rdd_5 = rdd_4.putInIndex(list1).cache()
	print(rdd_5.collect())
	print(rdd_5.getNumPartitions())
	


	print("DEL Output *******************************************************")
	rdd_6 = rdd_5.deleteFromIndex(7)
	print(rdd_6.collect())
	print(rdd_6.getNumPartitions())
	print(rdd_6.getFromIndex(7))
	print("DEL Output *******************************************************") 

	print("Filter Output *******************************************************")
	rdd_7 = rdd_6.filter(lambda (x):(x[0]%2==0))
	print(rdd_7.collect())
	print("Filter Output *******************************************************")

	

	print("Join Output *******************************************************")
	rdd_7 = sc.parallelize(range(6,9)).map(lambda x:(x,x*x*x))
	rdd_8 = IndexedRDD.updatable(rdd_7)
	print("RDD2 *******************************************************")
	print(rdd_6.collect())
	print(rdd_8.collect())
	print("RDD8 *******************************************************")
	rdd_9 = rdd_6.fullOuterJoin(rdd_8,lambda (id,(a,b)):(id,(a,b)))
	print(rdd_9.collect())
	print(rdd_9.getNumPartitions())
	print("Join Output *******************************************************") 

	


	

if __name__ == "__main__":
	main()