import sys
sys.path.append("GX1164.jar")

import com.ankurdave.part.ArtTree



# private[indexedrdd] class PARTPartition[K, V]

#     (protected val map: ArtTree)
#     (override implicit val kTag: ClassTag[K],
#      override implicit val vTag: ClassTag[V],
#      implicit val kSer: KeySerializer[K])
#   extends IndexedRDDPartition[K, V] with Logging {

#   protected def withMap[V2: ClassTag]
#       (map: ArtTree): PARTPartition[K, V2] = {
#     new PARTPartition(map)
#   }

#   override def size: Long = map.size()

#   override def apply(k: K): V = map.search(kSer.toBytes(k)).asInstanceOf[V]

#   override def isDefined(k: K): Boolean = map.search(kSer.toBytes(k)) != null

#   override def iterator: Iterator[(K, V)] =
#     map.iterator.map(kv => (kSer.fromBytes(kv._1), kv._2.asInstanceOf[V]))

#   private def rawIterator: Iterator[(Array[Byte], V)] =
#     map.iterator.map(kv => (kv._1, kv._2.asInstanceOf[V]))

#   override def multiget(ks: Iterator[K]): Iterator[(K, V)] =
#     ks.flatMap { k => Option(this(k)).map(v => (k, v)) }

#   override def multiput[U](
#       kvs: Iterator[(K, U)], z: (K, U) => V, f: (K, V, U) => V): IndexedRDDPartition[K, V] = {
#     val newMap = map.snapshot()
#     for (ku <- kvs) {
#       val kBytes = kSer.toBytes(ku._1)
#       val oldV = newMap.search(kBytes).asInstanceOf[V]
#       val newV = if (oldV == null) z(ku._1, ku._2) else f(ku._1, oldV, ku._2)
#       newMap.insert(kBytes, newV)
#     }
#     this.withMap[V](newMap)
#   }

#   override def delete(ks: Iterator[K]): IndexedRDDPartition[K, V] = {
#     val newMap = map.snapshot()
#     for (k <- ks) {
#       newMap.delete(kSer.toBytes(k))
#     }
#     this.withMap[V](newMap)
#   }

#   override def mapValues[V2: ClassTag](f: (K, V) => V2): IndexedRDDPartition[K, V2] = {
#     val newMap = new ArtTree
#     for (kv <- rawIterator) newMap.insert(kv._1, f(kSer.fromBytes(kv._1), kv._2))
#     this.withMap[V2](newMap)
#   }
# }


"""private[indexedrdd] object PARTPartition {
  def apply[K: ClassTag, V: ClassTag]
      (iter: Iterator[(K, V)])(implicit kSer: KeySerializer[K]) =
    apply[K, V, V](iter, (id, a) => a, (id, a, b) => b)

  def apply[K: ClassTag, U: ClassTag, V: ClassTag]
      (iter: Iterator[(K, U)], z: (K, U) => V, f: (K, V, U) => V)
      (implicit kSer: KeySerializer[K]): PARTPartition[K, V] = {
    val map = new ArtTree
    iter.foreach { ku =>
      val kBytes = kSer.toBytes(ku._1)
      val oldV = map.search(kBytes).asInstanceOf[V]
      val newV = if (oldV == null) z(ku._1, ku._2) else f(ku._1, oldV, ku._2)
      map.insert(kBytes, newV)
    }
    new PARTPartition(map)
  }
}"""

class PARTPartition:

  def apply(iter , z = lambda K, U : V, f= lambda K, V, U : V):
    map = ArtTree()
    for ku in enumerate(iter):
      kBytes = kSer.toBytes(ku._1)
      oldV = map.search(kBytes).asInstanceOf[V]
       
      if (oldV == null): 
        newV = z(ku._1, ku._2) 
      else: 
        newV = f(ku._1, oldV, ku._2)

      map.insert(kBytes, newV)
    """iter.foreach { ku =>
      val kBytes = kSer.toBytes(ku._1)
      val oldV = map.search(kBytes).asInstanceOf[V]
      val newV = if (oldV == null) z(ku._1, ku._2) else f(ku._1, oldV, ku._2)
      
    }"""
    PARTPartition(map)
  


