import org.apache.spark.ml.regression.LinearRegression

// Load training data
val training = spark.read.format("libsvm")
  .load("data/mllib/sample_linear_regression_data.txt")

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(training)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Summarize the model over the training set and print out some metrics
val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")

==============================================================
==============================================================
==============================================================
==============================================================
==============================================================



val path = "file:///data0/work_space/shixi_enzhao/sparkResourcesLimitProbing/sprt/linearRegression"
val file = path + "/sample_linear_regression_data.txt"
// val training = spark.read.format("libsvm").load(file) 

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

// Load and parse the data
val data = sc.textFile(file)
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}.cache()

// Building the model
val numIterations = 100
val stepSize = 0.00000001
val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
println("training Mean Squared Error = " + MSE)

==============================================================
==============================================================
==============================================================
==============================================================
==============================================================


val file = """D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\FrameCommon\LimitProbing\PressureTest\rst\Limit Probing.csv"""
val data = sc.textFile(file).map(_.split(',').map(x => x.toDouble))
data.first

val rst = Array.fill[(String, Double)](6)(""->0d)
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
// Load and parse the data
val parsedData = data.map { line =>
  val label = line.head
  val features = Vectors.dense(line.tail)
  LabeledPoint(label, features)
}.cache()

val posData = parsedData.filter(_.label >= 0)
val negData = parsedData.filter(_.label < 0)


// Building the model
val numIterations = 1000000
val stepSize = 0.00000001
val stepSize = 1e-4
val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
val posModel = LinearRegressionWithSGD.train(posData,numIterations,stepSize)
val negModel = LinearRegressionWithSGD.train(negData,numIterations,stepSize)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
println("training Mean Squared Error = " + MSE)
rst(0) = ("MSE" -> MSE)
val posVAP = posData.map { point =>
  val prediction = posModel.predict(point.features)
  (point.label, prediction)
}
val posMSE = posVAP.map{ case(v,p) => math.pow((v - p), 2) }.mean
rst(1) = ("posMSE" -> posMSE)
val negVAP = negData.map { point =>
  val prediction = negModel.predict(point.features)
  (point.label, prediction)
}
val negMSE = negVAP.map{ case(v,p) => math.pow((v - p), 2) }.mean
rst(2) = ("negMSE" -> negMSE)


import org.apache.spark.mllib.regression.RidgeRegressionWithSGD

// Building the model
val regParam = 0.1
val ridgeModel = RidgeRegressionWithSGD.train(parsedData, numIterations, stepSize, regParam)
val posRidgeModel = RidgeRegressionWithSGD.train(posData,numIterations,stepSize, regParam)
val negRidgeModel = RidgeRegressionWithSGD.train(negData,numIterations,stepSize, regParam)

// Evaluate model on training examples and compute training error
val ridgeValuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val ridgeMSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
println("training Mean Squared Error = " + MSE)
rst(3) = ("ridgeMSE" -> ridgeMSE)
val posRidgeVAP = posData.map { point =>
  val prediction = posModel.predict(point.features)
  (point.label, prediction)
}
val posRidgeMSE = posRidgeVAP.map{ case(v,p) => math.pow((v - p), 2) }.mean
rst(4) = ("posRidgeMSE" -> posRidgeMSE)
val negRidgeVAP = negData.map { point =>
  val prediction = negModel.predict(point.features)
  (point.label, prediction)
}
val negRidgeMSE = negRidgeVAP.map{ case(v,p) => math.pow((v - p), 2) }.mean
rst(5) = ("negRidgeMSE" -> negRidgeMSE)


val rstSqrtMean = rst.map( x => s"sqrt(${x._1})" -> math.sqrt(x._2))

val rstError = rst.clone
val ErrorMSE = valuesAndPreds.map{ case(v, p) => math.sqrt(math.pow((v - p), 2)) }.mean()
val posErrorMSE = posVAP.map{ case(v,p) => math.sqrt(math.pow((v - p), 2)) }.mean
val negErrorMSE = negVAP.map{ case(v,p) => math.sqrt(math.pow((v - p), 2)) }.mean
val ridgeErrorMSE = valuesAndPreds.map{ case(v, p) => math.sqrt(math.pow((v - p), 2)) }.mean()
val posRidgeErrorMSE = posRidgeVAP.map{ case(v,p) => math.sqrt(math.pow((v - p), 2)) }.mean
val negRidgeErrorMSE = negRidgeVAP.map{ case(v,p) => math.sqrt(math.pow((v - p), 2)) }.mean
rstError(0) = ("ErrorMSE " -> ErrorMSE )
rstError(1) = ("posErrorMSE " -> posErrorMSE )
rstError(2) = ("negErrorMSE " -> negErrorMSE )
rstError(3) = ("ridgeErrorMSE " -> ridgeErrorMSE )
rstError(4) = ("posRidgeErrorMSE " -> posRidgeErrorMSE )
rstError(5) = ("negRidgeErrorMSE " -> negRidgeErrorMSE )


println("---rst---")
rst.foreach(println)
println("---rstSqrtMean---")
rstSqrtMean.foreach(println)
println("---rstError---")
rstError.foreach(println)

val test3Y = Vectors.dense(Array(  3,10,5,40,4,7,7d))
val prediction3Y = model.predict(test3Y)