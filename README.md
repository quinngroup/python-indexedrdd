# Python IndexedRDD

IndexedRDD extends RDD[(K, V)] and pre-indexes the entries imposing unique keys to facilitate efficient point lookups, updates and deletion. It:
 	- hash-partitions the entries based on the keys
 	- follows a dictionary structure within wacg partition

# Installation

Before using IndexedRDD, take a quick look at the license and make sure you're OK with their terms.

Since, Python IndexedRDD isn't published as a package, download the folder to a location of your choice, and run the setup.py inside.

For example:
```
wget https://github.com/quinngroup/python-indexedrdd/archive/master.zip -O python-indexedrdd.zip
unzip -q python-indexedrdd.zip
cd python-indexedrdd-master/
python setup.py clean build install

# You're good to go!
```
# Usage

Once installed you can use Python IndexedRDD as follows:

```
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

```


