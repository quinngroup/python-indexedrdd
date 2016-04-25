class MyHashMap:

	def __init__(self,hashMap):
		
		d1=dict()
		for k,v in hashMap:
			d1[k]=v
		
		self.x = d1
		self.count = 0
		
    
	def __iter__(self):
		return(self.x.iteritems())

	def next(self): 
    		if self.count==0:
    			self.count=1
    			return self.x
    		else:
    			raise StopIteration

   	def get(self,key):
   		return self.x[key]

   	def put(self,keyList):
   		if (keyList[0] in self.x):	
   			del self.x[keyList[0]]

   		self.x[keyList[0]]=keyList[1]	
   		return (self)
   		
	def delete(self,keyList):
   		if (keyList in self.x):	
   			del self.x[keyList]

   		return (self)
   		