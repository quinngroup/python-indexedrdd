class MyList:


	def __init__(self,hashMapObject):
		self.x=list()
		self.x.append(hashMapObject)

	def __iter__(self):
		return(self.x[0].__iter__())

	