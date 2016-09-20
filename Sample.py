from indexedrdd import IndexedRDD
from pyspark import SparkContext

def main():
		sc = SparkContext("local", "IndexedRDD App")

		#Create an RDD of key-value pairs with Long keys
		initRdd = sc.parallelize(range(10000001),5).map(lambda x: (long(x), x*x))
		indexedRDD1 = IndexedRDD(initRdd).cache()

		#Get the value corresponding to a key: [1000000]
		print indexedRDD1.get(1000000)
		print "Default value!!!"
		
		#Update an existing value
		putRDD = indexedRDD1.put((1000000,1000001)).cache()
		print putRDD.get(1000000)
		 
		#Efficiently join derived IndexedRDD with original.
		indexedRDD2 = putRDD.join(indexedRDD1,lambda (id,(a,b)):(id,(a,b)))
		print indexedRDD2.get(1000000)

		#Perform insertions and deletions.
		indexedRDD3 = indexedRDD2.put(10000001, 111).delete(0).cache()
		print indexedRDD3.get(0)
		print indexedRDD3.get(10000001)
		

if __name__ == "__main__":
	main()
	