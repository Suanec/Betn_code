import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
// import org.apache.spark.
// import org.apache.spark.
// import org.apache.spark.
// import org.apache.spark.


  def rddRepeat( _rdd : RDD[_], _targetNum : Long ) : RDD[_] = {
    val srcSize = _rdd.count
    val multiple = _targetNum / srcSize
    val dataArr = Array.fill(multiple.toInt)(_rdd)
    val multiData = dataArr.reduce(_ union _)
    multiData
  }

val thousands =    1000 -1
val tenThousands = 10000 -1
val doubleTT =     100000 -1
val million =      1000000 -1
val tenMillion =   10000000 -1
val doubleTM =     100000000 -1
val indices = Array(doubleTT)
val value = Array(1d)
val sv = Vectors.sparse(indices.head+1,indices,value)
val dv = sv.toDense
val lbp = new LabeledPoint(1,v)
val rdd = sc.parallelize(Array.fill(100)(lbp)).cache()

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(rdd)
