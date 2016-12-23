package com.weibo.datasys.common

import org.apache.spark.rdd.RDD

/**
  * Created by wulei3 on 16/10/25.
  */

object PosNegSampling extends Serializable {

  @transient
  def startSampling(original:RDD[String], ratio:String):RDD[String] = {

    val posRDD = original.filter{
      case s:String =>
        s.split("\\s+").head.toDouble == 1.0
    }

    val negRDD = original.filter{
      case s:String =>
        s.split("\\s+").head.toDouble == 0.0
    }

    val posCount:Int = posRDD.count.toInt
    val negCount:Int = negRDD.count.toInt

    val posNegRatio:Int = ratio.split(":").last.toInt / ratio.split(":").head.toInt

    val targetNegCount:Int = posCount * posNegRatio
    val negSampleRatio:Double = targetNegCount / negCount.toDouble

    val targetPosCount:Int = negCount / posNegRatio
    val posSampleRatio:Double = targetPosCount / posCount.toDouble

    val Array(sampledRDD, _) = if(negSampleRatio < 1) negRDD.randomSplit(Array(negSampleRatio, 1 - negSampleRatio))
    else posRDD.randomSplit(Array(posSampleRatio, 1 - posSampleRatio))

    val unionRDD = if(negSampleRatio < 1) posRDD.union(sampledRDD)
    else negRDD.union(sampledRDD)

    val parentRDDPartitionNums:Int = original.getNumPartitions
    //val targetRDDPartitionNums:Int = parentRDDPartitionNums * unionRDD.count.toInt / original.count.toInt
    val targetRDDPartitionNums:Int = (parentRDDPartitionNums * unionRDD.count.toDouble / original.count.toInt
      ) < 1 match {
      case true => 1
      case false => (parentRDDPartitionNums * unionRDD.count.toDouble / original.count.toInt).toInt
    }

    unionRDD.coalesce(targetRDDPartitionNums)
  }

  @transient
  def startSamplingFlag(original:RDD[String], ratio:String) : RDD[String] = {
    original
  }

  @transient
  /**
   * sample, format repeat
   * @param thr     -[in]   the divide threshold value both action number and export number of the repeat instances
   * @param rthr    -[in]   the average interaction rate of all weibo
   */
   def startSamplingRepeat(original:RDD[String], ratio:String, thr:Int, rthr:Float) : RDD[String] = {
    val featureLabel : RDD[(String,Int)] = original.map{
      line =>
        val splits = line.split(' ')
        val label = splits.head.toInt
        val features = splits.tail.mkString(" ")
        features -> label
    }
    val featureLabels = featureLabel.groupByKey
    val repeatInstances = featureLabels.map(e => (e._2.sum, e._2.size, e._1))
    val sampleInstances = repeatInstances.map(e => {
      var act = 0
      var exp = 0
      if (e._1 >= thr){
        act = e._1 / thr
        exp = e._2 / thr
      } else {
        val rate = e._1.toFloat / (e._2 + 1)
        if (rate > rthr) {
          act = 1
        } else {
          act = 0
        }
        exp = 1
      }
      (act.toInt, exp.toInt, e._3)
    })
    val resultInstance = sampleInstances.flatMap{
      repeatLine =>
        val posCount = repeatLine._1
        val negCount = repeatLine._2 - posCount
        val posRst = Array.fill[String](posCount)("1 " + repeatLine._3)
        val negRst = Array.fill[String](negCount)("0 " + repeatLine._3)
        posRst union negRst
    }
    resultInstance
  }
}

==============================================================
==============================================================
==============================================================
==============================================================
==============================================================
val path = """D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\LogisticsRegression\rst\data.libsvm-2016-10-31-20-44-38"""
val data = sc.textFile(path).map{
      line =>
        val splits = line.split(' ')
        val label = (splits.head.toInt > 0) match {
          case true => 1
          case false => 0
        }
        val features = splits.tail.mkString(" ")
        features -> label
    }
val fls = data.groupByKey
val ris = fls.map(e => (e._2.sum, e._2.size, e._1))
val thr = 10
val rthr = 0.01
val sis = ris.map(e => {
  var act = 0
  var exp = 0
  if (e._1 >= thr){
    act = e._1 / thr
    exp = e._2 / thr
  } else {
    val rate = e._1.toFloat / (e._2 + 1)
    if (rate > rthr) {
      act = 1
    } else {
      act = 0
    }
    exp = 1
  }
  (act.toInt, exp.toInt, e._3)
})

val rst = sis.flatMap{
  repeatLine =>
    val posCount = repeatLine._1
    val negCount = repeatLine._2 - posCount
    val posRst = Array.fill[String](posCount)("1 " + repeatLine._3)
    val negRst = Array.fill[String](negCount)("0 " + repeatLine._3)
    posRst union negRst
}