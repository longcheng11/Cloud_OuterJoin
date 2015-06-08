/*
 * Knowledge Systems Group at TU Dresden
 * @Author Long Cheng
 * Copyright (c) 2015.
 */

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import scala.collection._
import org.apache.spark._
import scala.reflect.ClassTag

/*
 * This function is used for setting a hash partitioner to a RDD, without any re-partitioning/shuffle process
 */
class HashedRDD[T: ClassTag](
                              var prev: RDD[T],
                              n: Int)
  extends RDD[T](prev) {

  override val partitioner = if (n > 0) Some(new HashPartitioner(n)) else None

  override def getPartitions: Array[Partition] = prev.partitions

  override def getPreferredLocations(s: Partition): Seq[String] = prev.preferredLocations(s)

  override def getDependencies: Seq[Dependency[_]] = prev.dependencies

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = prev.iterator(split, context)

}

