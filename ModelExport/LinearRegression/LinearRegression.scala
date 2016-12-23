package com.weibo.datasys.algorithms

import com.weibo.datasys.common.HDFSFileLoader
import com.weibo.datasys.pipeline.MLRunnable
import org.apache.spark.ml.evaluation.{RegressionEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}


/**
  regParam_list  * Created by wulei3 on 16/12/22.
*/


object LinearRegression extends MLRunnable{

  def run(spark:SparkSession, input:AnyRef, conf:Map[String,String]):AnyRef = {

    val trainPath:String = conf("trainPath")
    val testPath:String  = conf("testPath")
    val model_path:String = conf("modelPath")
    //    val regCategory:String = conf("regCategory")
    val regParam_list:String = conf("regParam")
    val elasticNetParam_list:String = conf("elasticNetParam")
    val regParam:Array[Double] = regParam_list.split(",").map(_.toDouble).array
    val elasticNetParam:Array[Double] = elasticNetParam_list.split(",").map(_.toDouble).array

    // Prepare training and test data.
    /**
    val data = if(input == null) spark.read.format("libsvm").load(input_path)
      else input.asInstanceOf[DataFrame]
      */
    //val data = spark.read.format("libsvm").load(input_path)
    import spark.implicits._
    val trainRDD = HDFSFileLoader.loadLibsvmHDFS(spark, trainPath).map{
      case lp:LabeledPoint =>
        (lp.label, Vectors.dense(lp.features.toArray))
    }
    val testRDD = HDFSFileLoader.loadLibsvmHDFS(spark, testPath).map{
      case lp:LabeledPoint =>
        (lp.label, Vectors.dense(lp.features.toArray))
    }
    val trainData = trainRDD.toDF("label", "features")
    val testData = testRDD.toDF("label", "features")

    val liR = new LinearRegression()

    import org.apache.spark.ml.regression.GeneralizedLinearRegression

//    GeneralizedLinearRegression is support 4096 features in this spark,so we do not choose this impletion.
//    val glr = new GeneralizedLinearRegression()
//      .setFamily("gaussian")
//      .setLink("identity")
//      .setMaxIter(10)
//      .setRegParam(0.3)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.


    val paramGrid = new ParamGridBuilder()
      .addGrid(liR.regParam, regParam)
      .addGrid(liR.fitIntercept)
      //.addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(liR.elasticNetParam, elasticNetParam)
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(liR)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(trainData).bestModel.asInstanceOf[LinearRegressionModel]

    /**
    Make predictions on test data. model is the model with combination of parameters
      that performed best. */
    val trainingSummary = model.summary
    println(s"model w : ${model.coefficients.toArray.mkString("\n")}")
    println(s"model b : ${model.intercept}")
    println(s"train path : ${trainPath}")
    println(s"test path : ${testPath}")
    println(s"traindata : ${trainData.head}")
    println(s"data : ${trainData.count}, ${testData.count}")
    println(s"regs : ${regParam_list}, ${elasticNetParam_list}")
    println(s"Best model's MAE: ${trainingSummary.meanAbsoluteError}")
    println(s"Best model's MSE: ${trainingSummary.meanSquaredError}")
    println(s"Best model's RMSE: ${trainingSummary.rootMeanSquaredError}")
    trainingSummary.residuals.show()
    println(s"Best model's r2: ${trainingSummary.r2}")

    /** Save the best model to local file system. */
    model2file(model, model_path)

    /** Debug
    println("\nscoreAndLabels: ")
    scoreAndLabels.take(50).foreach(println)
    val precision:Double = scoreAndLabels.filter(x => x._1 == 1.0 && x._2 == 1.0).count /
      scoreAndLabels.filter(x => x._1 == 1.0).count.toDouble
    val recall:Double    = scoreAndLabels.filter(x => x._1 == 1.0 && x._2 == 1.0).count /
      scoreAndLabels.filter(x => x._2 == 1.0).count.toDouble
    println("\ntest AUC: " + auROC + "\ntest AUPR:" + metrics.areaUnderPR() +
      "\ntest Precision: " + precision + "\ntest Recall: " + recall)
    println("\nBest model's params: " + model.bestModel.extractParamMap())
      */
    println("\nModel has been saved to local file system: " + model_path + "\n")
    0.asInstanceOf[AnyRef]
  }

  def model2file(_lirModel : org.apache.spark.ml.regression.LinearRegressionModel,
                 _outPath : String) : Unit = {
    val writer = new java.io.PrintWriter(_outPath,"utf-8")
    var rst = ""
    rst += "solver_type " + "LinearRegression\n"
    rst += "nr_feature " + _lirModel.coefficients.size.toString + "\n"
    rst += "bias " + _lirModel.intercept.toString + "\n"
    writer.write(rst)
    rst = ""
    rst += "w \n"
    writer.write(rst)
    rst = ""
    writer.flush
    rst += _lirModel.coefficients.toArray.mkString("\n")
    writer.write(rst)
    rst = ""
    writer.flush
    writer.close
  }

}