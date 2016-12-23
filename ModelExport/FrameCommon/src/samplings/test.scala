package sample

/**
 * Created by xiangdong5 on 16/12/19.
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: sample.test <input> <output> <divide-threshold> <rate-threshold>")
      System.exit(1)
    }
    //实例化spark的上下文环境
    val conf = new SparkConf().setAppName("interaction-rate-sample")//.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sampler = new InteractionRateSampler()
    val resultRdd = sampler.sample(sc, args(0), args(1), args(2).toInt, args(3).toFloat)
    //resultRdd.foreach(println)
  }
}
