/*
 * Knowledge Systems Group at TU Dresden
 * @Author Long Cheng
 * Copyright (c) 2015.
 */

import org.apache.spark.util.collection.AppendOnlyMap
import scala.collection.mutable.ArrayBuffer

/*
 * This function is used for local joins, similar as the group operations in Spark.
 * (k,a) group with (k,b) results in (k, iter(a), iter(b))
 */
class CoGroupTable[Long](r: Iterator[(Long, String)], s: Iterator[(Long, String)]) {
  private type CoGroup = ArrayBuffer[String]
  private type CoGroupCombiner = Array[CoGroup]

  def compute(): Iterator[(Long, Array[Iterable[String]])] = {
    val map = new AppendOnlyMap[Long, CoGroupCombiner]
    val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
      if (hadVal) oldVal else Array.fill(2)(new CoGroup)
    }
    val getCombiner: Long => CoGroupCombiner = key => {
      map.changeValue(key, update)
    }

    r.foreach { kv =>
      getCombiner(kv._1)(0) += kv._2
    }

    s.foreach { kv =>
      getCombiner(kv._1)(1) += kv._2
    }

    map.iterator.asInstanceOf[Iterator[(Long, Array[Iterable[(String)]])]]
  }
}
