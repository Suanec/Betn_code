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

==============================================================
==============================================================
==============================================================
==============================================================
==============================================================
val wsk = """D:\DPartition\ProgramData\Scala\spark-2.0.1-bin-hadoop2.6\data\mllib"""
val path = wsk + """\sample_linear_regression_data.txt"""
val regParam = Array(0.1,0.3,0.5)
val elasticNetParam = Array(0.8,0.8,0.8)
val trainData = spark.read.format("libsvm").load(path)
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.{RegressionEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
val liR = new LinearRegression()
val paramGrid = new ParamGridBuilder()
  .addGrid(liR.regParam, regParam)
  .addGrid(liR.fitIntercept)
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
// val model = trainValidationSplit.fit(trainData)
// val predictAndLabel = model.transform(data).select("label","prediction").map{
//   r =>
//     math.pow((r.getDouble(0) - r.getDouble(1)), 2)
// }
// val MSE = predictAndLabel.reduce(_ + _) / predictAndLabel.count
// val RMSE = math.sqrt(MSE)
val model = trainValidationSplit.fit(trainData).bestModel.asInstanceOf[LinearRegressionModel]

/**
Make predictions on test data. model is the model with combination of parameters
  that performed best. */
val trainingSummary = model.summary
println(s"Best model's MAE: ${trainingSummary.meanAbsoluteError}")
println(s"Best model's MSE: ${trainingSummary.meanSquaredError}")
println(s"Best model's RMSE: ${trainingSummary.rootMeanSquaredError}")
trainingSummary.residuals.show()
println(s"Best model's r2: ${trainingSummary.r2}")
==============================================================
==============================================================
==============================================================
==============================================================
==============================================================
import com.weibo.datasys._
import com.weibo.datasys.etl._
import com.weibo.datasys.pipeline._
import com.weibo.datasys.macros._
import com.weibo.datasys.common._
import XmlConfig._
val strXML = """
<configuration>
    <spark>
        <property name="">""</property>
    </spark>
    <pipeline> 
        <!-- pipeline=处理流程配置 -->
        <stage name="LinearRegression">
            <process id="1" preid="-1">LinearRegression</process>
        </stage>
    </pipeline>
    <processes>
        <!-- process=每个步骤/阶段处理细节配置 -->
        <process name="LinearRegression">
            <objectname>com.weibo.datasys.algorithms.LinearRegression</objectname>
            <args>
                <arg name="trainPath">D:\DPartition\ProgramData\Scala\spark-2.0.1-bin-hadoop2.6\data\mllib\sample_linear_regression_data.txt</arg>
                <arg name="testPath">D:\DPartition\ProgramData\Scala\spark-2.0.1-bin-hadoop2.6\data\mllib\sample_linear_regression_data.txt</arg>
                <arg name="modelPath">D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\LinearRegression\linearRegression.model</arg>
                <arg name="regParam">0.1,0.3,0.5</arg>
                <arg name="elasticNetParam">0.8,0.8,0.8</arg>
            </args>
        </process>
    </processes>
</configuration>
"""
val configFile = xml.XML.loadString(strXML)
val pipelineConf = XmlConfig.loadPipelineConf(configFile)
val pipelineRange = "[1]"
val (pipelineName, pipelineCon) = pipelineConf.head
println("Running pipeline: " + pipelineName + "\n")
pipelineCon.run(spark, pipelineRange)

