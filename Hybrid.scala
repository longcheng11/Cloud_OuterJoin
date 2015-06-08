/*
 * Knowledge Systems Group at TU Dresden
 * @Author Long Cheng
 * Copyright (c) 2015.
 */


import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer

object HAOuterJoin {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "HAOuterJoin", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))
    val K = args(3).toInt //top K skewed keys

    // R is smaller, so broadcast it as a map<String, String>
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
    val results1 = r_dis.leftOuterJoin(s_dis)
    println("HAOuterJoin number of result1 is " + results1.count())

    //DER implementation for the rest part
    val broadCastR = sc.broadcast(r_dup.collect())

    val N = s_loc.partitions.size //number of partitions for final id checking

    // S inner joins with R in map side, R access S actually
    val join_1 = s_loc.mapPartitions({ iter =>
      //building the hashtable for S
      var table = new HashMap[Long, ListBuffer[String]]
      var results_1 = new ListBuffer[(Long, ListBuffer[String])]
      var m = broadCastR.value
      while (iter.hasNext) {
        var record = iter.next
        var value = table.getOrElse(record._1, null)
        if (value == null) {
          table += record._1 -> (new ListBuffer[String] += record._2)
        }
        else {
          value += record._2
        }
      }

      //hash join and also record the non-matched ones
      var i: Long = 0
      for ((key, value) <- m.iterator)
      yield {
        i += 1
        var value1 = table.getOrElse(key, null)
        if (value1 == null)
          (i, None) //non-matched
        else
          (key, (value, value1)) //matched
      }
    })

    //the matched results
    var matched = join_1.filter(x => x._2 != None)
    println("HAOuterJoin number of result2 is " + matched.count())

    //the keys of the non-matched part, and get the keys with number == N
    var non_keys = join_1.filter(x => x._2 == None).map(tuple => (tuple._1, 1)).reduceByKey(_ + _).filter(key => key._2 == N) //(k1)

    //the keys search the broadcast R
    var non_matched = non_keys.mapPartitions({ iter =>
      var m = broadCastR.value
      for (pos <- iter) yield {
        var pair = m.apply(pos._1.toInt - 1)
        (pair._1, pair._2, None)
      }
    })
    println("HAOuterJoin number of result3 is " + non_matched.count())

    sc.stop()
  }
}
