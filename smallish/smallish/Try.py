from pyspark import SparkContext
from IndexedRDD import IndexedRDD
import time

def main():

	sc = SparkContext("local", "IndexedRDD App")
	
	print("Class initialization *******************************************************")
	initRdd = sc.parallelize(range(100000)).map(lambda x: (x, x*x))
	partitionedRDD = initRdd.partitionBy(5)
	indexedRDD = IndexedRDD.updatable(partitionedRDD)
	indexedRDD1 = IndexedRDD(indexedRDD)
	print(indexedRDD1.take(1))
	print("Class initialization *******************************************************")



	print("Before GET Output *******************************************************")
	print(time.time() ,  " Time before get")
	print(indexedRDD1.get(15))
	print(time.time() ,  " Time after get")
	print("After GET Output *******************************************************")
	
	
	
	print("PUT Output *******************************************************")
	print (indexedRDD1.get(25))
	print(time.time() ,  " Time before put")
	putRDD = indexedRDD1.put((25,123)).cache()
	print(time.time() ,  " Time after put")
	print (putRDD.get(25))
	print("PUT Output *******************************************************")

	

	print("DEL Output *******************************************************")
	print (putRDD.get(7))
	print(time.time() ,  " Time before DELETE")
	delRDD = putRDD.delete(7)
	print(time.time() ,  " Time after DELETE")
	print (delRDD.get(7))
	print("DEL Output *******************************************************") 


	print("Filter Output *******************************************************")
	filterRDD = delRDD.filter(lambda (x):(x[0]%2==0))
	print(filterRDD.take(2))
	print("Filter Output *******************************************************")

	print("Join Output *******************************************************")
	rddX = sc.parallelize(range(10,21)).map(lambda x:(x,x*x*x))
	rddY = IndexedRDD.updatable(rddX.partitionBy(5))
	rddZ = IndexedRDD(rddY)
	print(time.time() ,  " Time before join")
	joinRDD = delRDD.leftJoin(rddZ,lambda (id,(a,b)):(id,(a,b)))
	print(time.time() ,  " Time after join")
	print(joinRDD.take(1))
	print("Join Output *******************************************************")
	
	if __name__ == "__main__":
		main()
		
		