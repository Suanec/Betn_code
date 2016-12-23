package sample

/**
 * Created by xiangdong5 on 16/12/19.
 */

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class InteractionRateSampler {

  /**
   * input the flag instance, first transform to repeat, after sample, transform them to flag
   *
   * @param sc      -[in]   SparkContext
   * @param input   -[in]   flag instances path of HDFS
   * @param output  -[out]  flag instances path of HDFS, after sampling
   * @param thr     -[in]   the divide threshold value both action number and export number of the repeat instances
   * @param rthr    -[in]   the average interaction rate of all weibo
   */
  def sample(sc:SparkContext, input:String, output:String, thr:Int, rthr:Float) : RDD[String] = {
    //#1 read data
    val instances = sc.textFile(input)
    //#2 flag to repeat
    val featureLabel = instances.map(inst => (inst.substring(2), inst.charAt(0) - 48))
    val featureLabels = featureLabel.groupByKey(50)
    val repeatInstances = featureLabels.map(e => (e._2.sum, e._2.size, e._1))
    //repeatInstances.saveAsTextFile(output + "/group")
    //#3 sample
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
    //sampleInstances.saveAsTextFile(output + "/sample")
    //#4 repeat to flag
    val resultInstance = sampleInstances.flatMap(e => {
      for (i <- 1 to e._2) yield {
        if (i <= e._1) {
          1.toString + " " + e._3
        } else {
          0.toString + " " + e._3
        }
      }
    })
    //#5 sava
    //resultInstance.saveAsTextFile(output + "/result")
    resultInstance.saveAsTextFile(output)

    return resultInstance
  }

}
