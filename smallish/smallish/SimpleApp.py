"""SimpleApp.py"""
from pyspark import SparkContext




def get1(elems,a):
	tempx=elems.collectAsMap()
	print(tempx)
	return tempx.get(a)

def updatable1(elems):
	t1=elems.collectAsMap()
	return t1

	"""#keys1=elems.keys()
	#temp = dict(elems)
	#tempx=temp.map(lambda default_data[x]: (x,0))
	for x in elems.collect():
		default_data[x]=x
		


	
	#	elemsPartitioned = elems
	#	elemsPartitioned = elems
	#	partitions = elemsPartitioned.mapPartitions(
    # testFunc,
    # True)
 	
 	#if (elems.partitioner.isDefined): 
  #		
  #else:
  #		elemsPartitioned = elems.partitionBy(new HashPartitioner(elems.partitions.size))"""
    
def main():

	logFile = "README.md"  # Should be some file on your system
	sc = SparkContext("local", "Simple App")
	logData = sc.textFile(logFile).cache()

	numAs = logData.filter(lambda s: 'a' in s).count()
	numBs = logData.filter(lambda s: 'b' in s).count()

	print("*******************************************************")
	print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
	print("*******************************************************")

	#Create an RDD of key-value pairs with Long keys.
	rdd = sc.parallelize(range(10)).map(lambda x: (x, x))
	#Original RDD
	t1=updatable1(rdd)
	t2=sc.parallelize(t1)
	print("*******************************************************")
	print(t2.first())
	print("*******************************************************")	  
	a=get1(t2,9)
	print(a)	  
	
	    

	

if __name__ == "__main__":
	main()