/*
 * Knowledge Systems Group at TU Dresden
 * @Author Long Cheng
 * Copyright (c) 2015.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.immutable.{HashMap, HashSet}

object PRPDOuterJoin {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "PRPDOuterJoin", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))
    val K = args(3).toInt // frequency >> K

    var r_pairs = R.map { x => var pos = x.split('|')
      (pos(0).toLong, pos(1).toString)
    }

    var s_pairs = S.map { x => var pos = x.split('|')
      (pos(0).toLong, pos(1).toString)
    }

    //Sample S and get the skewed keys
    var s_sample = s_pairs.sample(false, 0.1)
    var skew_keys = s_sample.mapPartitions({ iter =>
      var table = new HashMap[Long, Int]()
      while (iter.hasNext) {
        var key = iter.next()._1
        var value = table.getOrElse(key, -1)
        if (value != -1) {
          table += key -> (value + 1)
        }
        else {
          table += key -> 1
        }
      }
      table.iterator
    }).filter(x => (x._2 >= K)).map(y => y._1).collect()

    var set = new HashSet[Long]
    skew_keys.foreach(key => set += key)

    //broadcast the set
    val broadCastSet = sc.broadcast(set)

    //partition S
    var s_part = s_pairs.mapPartitions({ iter =>
      var m = broadCastSet.value
      for (pair <- iter) yield {
        if (m.contains(pair._1))
          (pair, 1)
        else
          (pair, 0)
      }
    })
    var s_loc = s_part.filter(x => x._2 == 1).map(x => x._1)
    var s_dis = s_part.filter(x => x._2 == 0).map(x => x._1)

    //partition R
    var r_part = r_pairs.mapPartitions({ iter =>
      var m = broadCastSet.value
      for (pair <- iter) yield {
        if (m.contains(pair._1))
          (pair, 1)
        else
          (pair, 0)
      }
    })
    var r_dup = r_part.filter(x => x._2 == 1).map(x => x._1)
    var r_dis = r_part.filter(x => x._2 == 0).map(x => x._1)

    //left-outer join between redistributed part
    var results1 = r_dis.leftOuterJoin(s_dis)
    println("PRPDOuterJoin number of result1 is "+results1.count())

    //duplication-based outer joins for r_dup and s_loc
    val broadCastMap = sc.broadcast(r_dup.collectAsMap)
    var Inner_Join = s_loc.mapPartitions({ iter =>
      var m = broadCastMap.value.iterator
      var cogroup = new CoGroupTable[Long](m, iter)
      cogroup.compute()
    }).flatMapValues({ pair =>
      for (v <- pair(0).iterator; w <- pair(1).iterator) yield (v, w)
    })

    var results2 = r_dup.leftOuterJoin(Inner_Join)
    println("PRPDOuterJoin number of result2 is "+results2.count())

    sc.stop()

  }
}
