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
  import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
  import org.apache.spark.mllib.evaluation.MulticlassMetrics
  import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
  import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
  import org.apache.spark.mllib.util.MLUtils

  def main(args : Array[String]) = {
    println("args : trainPath testPath iterNum")
    if(args.size != 3) {
      println("args size error!")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setAppName("trainLR")
    val sc = new SparkContext(conf)
    println(sc.getConf)

    val trainPaths = args.head
    val data = if(trainPaths.contains(",")) {
      println("Multi Paths!")
      trainPaths.split(',').map{
        x => MLUtils.loadLibSVMFile(sc,x)
      }.reduce(_ union _)
    } else {
      MLUtils.loadLibSVMFile(sc,trainPaths)
    }
    // val data = MLUtils.loadLibSVMFile(sc,args.head)
    println(s"train path : ${args.head} \ntest : ${args.init.last} \niterNum : ${args.last}")
    val test = MLUtils.loadLibSVMFile(sc,args.tail.head)

    // Run training algorithm to build the model
    println("Training...")

    val model = LogisticRegressionWithSGD.train(data,args.last.toInt)
    println("Training Done!")

    // Compute raw scores on the test set.
    println("Testing...")
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    println("Testing Done!")

  }
}

