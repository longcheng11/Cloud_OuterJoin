/*
 * Knowledge Systems Group at TU Dresden
 * @Author Long Cheng
 * Copyright (c) 2015.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object HashOuterJoin {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "HashOuterJoin", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))

    var r_pairs = R.map { x => var pos = x.split('|')
      (pos(0).toLong, pos(1).toString)
    }

    var s_pairs = S.map { x => var pos = x.split('|')
      (pos(0).toLong, pos(1).toString)
    }

    var join = r_pairs.leftOuterJoin(s_pairs)
    println("HashOuterJoin number of result is "+join.count())

    sc.stop()
  }
}
