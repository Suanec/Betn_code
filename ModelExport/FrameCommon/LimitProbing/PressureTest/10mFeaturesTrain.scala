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
// val indices = Array(doubleTT)
// val value = Array(1d)
// val sv = Vectors.sparse(indices.head+1,indices,value)
// val dv = sv.toDense
// val lbp = new LabeledPoint(1,v)
// val rdd = sc.parallelize(Array.fill(1)(lbp)).cache()

// // Run training algorithm to build the model
// val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(rdd)


// val rand = new scala.util.Random
// val featureSize = tenMillion + 1
// val data = (0 until featureSize).map(i => math.abs(rand.nextInt % featureSize)).distinct.sorted.toArray
// val sv = Vectors.sparse(featureSize,data,Array.fill[Double](data.size)(1d))
// val dv = sv.toDense
// val lbp = new LabeledPoint(1,dv)
// val dataSize = 1
// val rdd = sc.parallelize(Array.fill(1)(lbp)).cache
// val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(rdd)

val tenMillion =   10000000 -1
val rand = new scala.util.Random
val sparseSize = 1024
val featureSize = tenMillion + 1
// val data = (0 until featureSize).map(i => math.abs(rand.nextInt % featureSize)).distinct.sorted.toArray
val dataSize = 15

// new LogisticRegressionWithLBFGS().setNumClasses(2).run(sc.parallelize(Array.fill(dataSize)(new LabeledPoint(1,Vectors.sparse(featureSize,data,Array.fill[Double](data.size)(1d)).toDense))))
// new org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS().setNumClasses(2).run(sc.parallelize(Array.fill(dataSize)(new org.apache.spark.mllib.regression.LabeledPoint(1,org.apache.spark.mllib.linalg.Vectors.sparse(featureSize,data,Array.fill[Double](data.size)(1d))))))
def genVector(_sparseSize : Int, _featureSize : Int)  = {
  val dataT = (0 until _sparseSize).map(i => math.abs(rand.nextInt % _featureSize)).distinct.sorted.toArray
  org.apache.spark.mllib.linalg.Vectors.sparse(_featureSize,dataT,Array.fill[Double](dataT.size)(1d))
}
def testLimit(_dataSize : Int) = {
  org.apache.spark.mllib.classification.LogisticRegressionWithSGD.train(sc.parallelize(Array.fill(_dataSize)(new org.apache.spark.mllib.regression.LabeledPoint(1,genVector(featureSize)))),2)
  _dataSize
}
testLimit(8)
testLimit(10)//fail
println(s"dataSize : ${dataSize}")













