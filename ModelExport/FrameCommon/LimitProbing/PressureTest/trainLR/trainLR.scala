object trainLR {
  import org.apache.spark.mllib.linalg.Vector
  import org.apache.spark.mllib.util.MLUtils
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import scala.util.Random
  import scala.math.abs
  import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
  import org.apache.spark.mllib.evaluation.MulticlassMetrics
  def main(args : Array[String]) = {
    println("args : path")
    if(args.size != 1) System.exit(1)
    val conf = new SparkConf()
      .setAppName("trainLR")
    val sc = new SparkContext(conf)
    println(sc.getConf)

    val data = MLUtils.loadLibSVMFile(sc,args.head)
    val count = data.count
    println(s"path : ${args.head}, DataCount : ${count}")
    val testRate = 0.01
    val splits = data.randomSplit(Array(0.99,0.01),System.currentTimeMillis)
    val test = splits(1)
    println(test.count)

    // Run training algorithm to build the model
    println("Training...")

    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(data)
    println("Trianing Done!")

    // Compute raw scores on the test set.
    println("Testing...")
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    println("Testing Done!")

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println("LRacc : " -> metrics.accuracy)
    println(s"DataCount : ${count}")
  }
}

