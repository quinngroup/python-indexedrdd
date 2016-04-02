"""SimpleApp.py"""
from pyspark import SparkContext
from IndexedRDD import IndexedRDD

def main():


	print("Class initialization *******************************************************")
	rdd1 = IndexedRDD.initialize_method()
	rdd2 = IndexedRDD(rdd1)
	print(type(rdd2))
	print(rdd2.collect())
	print(rdd2.getNumPartitions())
	print("Class initialization *******************************************************")


	print("GET1 Output *******************************************************")
	list1=[(0,3)]
	print(rdd2.getFromIndex(list1))
	print("GET1 Output *******************************************************")

	print("Filter Output *******************************************************")
	rdd3 = rdd2.filter(lambda (x):(x[0]/1==1))
	print(rdd3.collect())
	print("Filter Output *******************************************************")

	
	print("PUT Output *******************************************************")
	rdd3 = IndexedRDD.initialize_method2()
	rdd4 = rdd2.putInIndex(rdd3).cache()
	print(rdd4.collect())
	print(rdd4.getNumPartitions())
	print(rdd4.getFromIndex([(0,5)]))
	print("PUT Output *******************************************************")


	print("DEL Output *******************************************************")
	rdd5 = rdd4.deleteFromIndex([(0,5)])
	print(rdd5.collect())
	print(rdd5.getNumPartitions())
	print(rdd5.getFromIndex([(0,5)]))
	print("DEL Output *******************************************************")


	print("Inner Join *******************************************************")
	rdd6 = IndexedRDD.initialize_method2()
	rdd7 = rdd5.innerJoin(rdd6,lambda (id,(a,b)):(id,(b,b)))
	print(rdd7.collect())
	print(rdd7.getNumPartitions())
	list1=[(0,7)]
	print(rdd7.getFromIndex(list1))
	
	#print(rdd5.getFromIndex([(0,5)]))
	print("Inner Join Output *******************************************************")


	

if __name__ == "__main__":
	main()