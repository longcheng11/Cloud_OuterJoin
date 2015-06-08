/*
 * Knowledge Systems Group at TU Dresden
 * @Author Long Cheng
 * Copyright (c) 2015.
 */

import org.apache.spark.SparkContext._
import scala.collection.immutable._
import org.apache.spark.SparkContext

object Query {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "Query", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))

    var r_pairs = R.map { x => var pos = x.split('|')
      (pos(0).toLong, pos(1).toString)
    }

    var s_pairs = S.map { x => var pos = x.split('|')
      (pos(0).toLong, pos(1).toString)
    }

    val n = s_pairs.partitions.size
    var s_hash = new HashedRDD(s_pairs, n) //set hash partitioner to S, so as to remove shuffle in the final join

    //collect the unique keys of each partition of S, record their location as idx
    var uni_keys = s_hash.mapPartitionsWithIndex({ (idx, iter) =>
      var keys = new HashMap[Long, Int] //key and the idx
      while (iter.hasNext) {
        var pair = iter.next()
        if (!keys.contains(pair._1)) {
          keys += pair._1 -> idx //add idx to the key
        }
      }
      keys.iterator
    })

    var join1 = r_pairs.leftOuterJoin(uni_keys)

    //the non-match results
    var non_matched = join1.filter(x => x._2._2 == None)
    println("QC number of result1 is "+non_matched.count())

    var match_key = join1.filter(x => x._2._2 != None).map(y => (y._2._2.get, (y._1, y._2._1))) //in the form of (idx, (k_R,v_R))), note that match_key has no preserved partitioner

    var part = s_hash.partitioner
    var part_key = match_key.partitionBy(part.get).mapPartitions({ iter =>
      for (tuple <- iter) yield (tuple._2._1, tuple._2._2) //remove the idx and keep the partitioner
    }, true)

    //both are have same hash partitioner, thus we do not move S at all in the joins.
    var matched = s_hash.join(part_key)
    println("QC number of result2 is "+matched.count())

    sc.stop()

  }
}
