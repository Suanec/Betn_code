package com.weibo.datasys.weispark.ml
/// @author : suanec_Betn
/// @version : 0.2
/// @data : 2016/10/24

import org.apache.spark.ml.classification.LogisticRegression

// // Load training data
// val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

// val lr = new LogisticRegression()
//   .setMaxIter(10)
//   .setRegParam(0.3)
//   .setElasticNetParam(0.8)

// // Fit the model
// val lrModel = lr.fit(training)

// // Print the coefficients and intercept for logistic regression
// println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

object LogisticsRegressionBasedDataFrame {
  def generateLRModel(
    _training : org.apache.spark.sql.DataFrame,
    _maxIter : Int, 
    _regParam : Double, 
    _elasticNetParam : Double) : org.apache.spark.ml.classification.LogisticRegressionModel = {
    new org.apache.spark.ml.classification.LogisticRegression()
      .setMaxIter(_maxIter)
      .setRegParam(_regParam)
      .setElasticNetParam(_elasticNetParam)
      .fit(_training)
  }
  def train(
    _filePath : String, 
    _maxIter : Int, 
    _regParam : Double, 
    _elasticNetParam : Double) : (org.apache.spark.ml.linalg.Vector,Double) = {
    val training = spark.read.format("libsvm").load(_filePath)
    val lr = new LogisticRegression()
      .setMaxIter(_maxIter)
      .setRegParam(_regParam)
      .setElasticNetParam(_elasticNetParam)

    val lrModel = lr.fit(training)

    lrModel.coefficients -> lrModel.intercept
  }
  def train( 
    _spark : org.apache.spark.sql.SparkSession,
    _filePath : String, 
    _maxIter : Int, 
    _regParam : Double, 
    _elasticNetParam : Double) : (org.apache.spark.ml.linalg.Vector,Double) = { 
    val training = _spark.read.format("libsvm").load(_filePath)
    val lrModel = generateLRModel(training,_maxIter,_regParam,_elasticNetParam)
    lrModel.coefficients -> lrModel.intercept
  }

  // Margin (rawPrediction) for class label 1.  For binary classification only. 
  // private val margin: Vector => Double = (features) => {
  //   BLAS.dot(features, coefficients) + intercept
  // }

  def model2file(
    _lrModel : org.apache.spark.ml.classification.LogisticRegressionModel,
    _outPath : String) : Unit = {
    val writer = new java.io.PrintWriter(_outPath,"utf-8")
    var rst = ""
    rst += "solver_type " + "L2R_LR\n"
    rst += "nr_class " + _lrModel.numClasses.toString + "\n"
    rst += "label " + "0 1" + "\n"
    rst += "nr_feature " + _lrModel.coefficients.size.toString + "\n"
    rst += "bias " + _lrModel.intercept.toString + "\n"
    writer.write(rst)
    rst = ""
    rst += "w \n"
    writer.write(rst)
    rst = ""
    writer.flush
    rst += _lrModel.coefficients.toArray.mkString("\n")
    writer.write(rst)
    rst = ""
    writer.flush
    writer.close
  }
}

