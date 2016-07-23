from indexedrdd import IndexedRDD
from pyspark import SparkContext
import cProfile

def main():
		#print ("here!!")
		
		sc = SparkContext("local", "IndexedRDD App")
		print("Class initialization *******************************************************")
		initRdd = sc.parallelize(range(1000000),5).map(lambda x: (x, x*x))
		indexedRDD1 = IndexedRDD(initRdd).cache()
		indexedRDD1.take(1)
		putRDD = indexedRDD1.put((1000001,1000001)).cache()
		putRDD.take(1)
		putRDD.get(1000001)
		 
		#initRdd2 = sc.parallelize([(1000001,1000001)])
		#indexedRDD2 = IndexedRDD(initRdd2).cache()
		#indexedRDD2.take(1)

		#indexedRDD3 = indexedRDD1.union(indexedRDD2)
		#indexedRDD3.take(1)
		#indexedRDD3.lookup(1000001)
		#initRdd.lookup(17)
		#partitionedRDD = initRdd.partitionBy(5)
		#
		#indexedRDD1.take(1)
		#print("Class initialization *******************************************************")

		#print("Before GET Output *******************************************************")
		#indexedRDD1.get(17)
		
		#0.330
		#indexedRDD1.lookup(17)
		
		#print("After GET Output *******************************************************")
		
		"""
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
		  print("Join Output *******************************************************")"""

if __name__ == "__main__":
	#main()
	cProfile.run('main()')
