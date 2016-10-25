// package weiSpark.ml.classification.NaiveBayes
package com.weibo.datasys.weispark.ml.Bayes

// import LibSVMUtil._

class weiNaiveBayesModel(m_trainFile : String,
                          m_modelPath : String
                          ) extends Serializable {
  var m_conf: org.apache.spark.SparkConf = new
      org.apache.spark.SparkConf()
    .setAppName("weiNaiveBayesModel.train")
    .setMaster("local")
  var m_sc: org.apache.spark.SparkContext = new
      org.apache.spark.SparkContext(m_conf)
  var m_sparkSession: org.apache.spark.sql.SparkSession =
      org.apache.spark.sql.SparkSession.builder.config(m_conf).getOrCreate

  def this(_inFile: String,
           _outFile: String,
           _conf: org.apache.spark.SparkConf) = {
    this(_inFile, _outFile)
    m_conf = _conf
    m_sc.stop
    m_sc = new org.apache.spark.SparkContext(m_conf)
    m_sparkSession.stop
    m_sparkSession = org.apache.spark.sql.SparkSession
      .builder
      .config(m_conf)
      .getOrCreate
  }

  def this(_inFile: String,
           _outFile: String,
           _sc: org.apache.spark.SparkContext) = {
    this(_inFile, _outFile)
    m_sc.stop
    m_sc = _sc
    m_conf = m_sc.getConf
    m_sparkSession.stop
    m_sparkSession = org.apache.spark.sql.SparkSession
      .builder()
      .config(m_conf)
      .getOrCreate()
  }

  def this(_inFile: String,
           _outFile: String,
           _sparkSession: org.apache.spark.sql.SparkSession) = {
    this(_inFile, _outFile)
    m_sparkSession.stop
    m_sparkSession = _sparkSession
    m_sc.stop
    m_sc = m_sparkSession.sparkContext
    m_conf = m_sc.getConf
  }

  def this(_inFile: String,
           _outFile: String,
           _appName: String,
           _master: String = "") = {
    this(_inFile, _outFile)
    m_conf = new org.apache.spark.SparkConf()
      .setAppName(_appName)
    if (_master.size > 0) m_conf.setMaster(_master)
    m_sc.stop
    m_sc = new org.apache.spark.SparkContext(m_conf)
    m_sparkSession.stop
    m_sparkSession = org.apache.spark.sql.SparkSession
      .builder()
      .config(m_conf)
      .getOrCreate()
  }

  def trainDF(_trainRate: Array[Double], _seed: Long = 123L):
    org.apache.spark.ml.classification.NaiveBayesModel = {
    if (_trainRate.size != 2) System.err.println(s"train Rate Error! ${_trainRate.size}")
    val m_data = m_sparkSession.read.format("libsvm").load(m_trainFile)
    val Array(m_trainingData, m_testData) = m_data.randomSplit(_trainRate, _seed)
    val m_model = new org.apache.spark.ml.classification.NaiveBayes()
      .fit(m_trainingData)
    val m_prediction = m_model.transform(m_testData)
    m_prediction.show

    // Select (prediction, true label) and compute test error
    val m_evaluator = new org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val m_accuracy = m_evaluator.evaluate(m_prediction)
    println("Accuracy: " + m_accuracy)
    // $example off$
    m_model
  }

  def trainDF():
    org.apache.spark.ml.classification.NaiveBayesModel = {
    trainDF(Array(0.8, 0.2), System.currentTimeMillis)
  }

  def trainRDD(_trainRate: Array[Double],
               _lambda: Double = 1d,
               _seed: Long = 123L,
               _modelType: String = "multinomial"):
    org.apache.spark.mllib.classification.NaiveBayesModel = {

    val m_data = org.apache.spark.mllib.util.MLUtils.loadLibSVMFile(m_sc, m_trainFile)

    // Split data into training (60%) and test (40%).
    val Array(m_training, m_test) = m_data.randomSplit(_trainRate, _seed)

    val m_model = org.apache.spark.mllib.classification.NaiveBayes
      .train(m_training, _lambda, _modelType)

    val m_predictionAndLabel = m_test.map(p => (m_model.predict(p.features), p.label))
    val m_accuracy = 1.0 * m_predictionAndLabel.filter(x => x._1 == x._2).count / m_test.count
    println("accuracy : " + ((m_accuracy * 10000).toInt / 100).toString)
    m_model
  }

  def trainRDD(): org.apache.spark.mllib.classification.NaiveBayesModel = {
    trainRDD(Array(0.8,0.2),1d,System.currentTimeMillis,"multinomial")
  }

  def getModel( _isDF : Boolean ) = {
    val rst_model = _isDF match {
      case true => trainDF()
      case false => trainRDD()
    }
    rst_model
  }

  def getModel( _isDF : Boolean,
                _trainingRate : Array[Double] = Array(0.8,0.2),
                _lambda : Double = 1d,
                _seed : Long = 123L,
                _modelType : String = "multinomial") = {
    val rst_model = _isDF match {
      case true => trainDF(_trainingRate,_seed)
      case false => trainRDD(_trainingRate,_lambda,_seed,_modelType)
    }
    rst_model
  }
}

object weiNaiveBayesModel {
  def trainModel( _dataFile : String ) : org.apache.spark.ml.classification.NaiveBayesModel = {
    val spark = org.apache.spark.sql.SparkSession
      .builder
      .appName("NaiveBayesExample")
      .getOrCreate()
    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").load(_dataFile)

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), System.currentTimeMillis)

    // Train a NaiveBayes model.
    val model = new
        org.apache.spark.ml.classification.NaiveBayes()
      .fit(trainingData)

    spark.stop()
    model
  }

  def trainModelRDD( _dataFile : String )
    : org.apache.spark.mllib.classification.NaiveBayesModel = {
    val weiNBC = new weiNaiveBayesModel(_dataFile,
      ""/*_dataFile.split('.').head + ".rst"*/,
      "NaiveBayesExample","local")
    weiNBC.getModel( false ).asInstanceOf[org.apache.spark.mllib.classification.NaiveBayesModel]
  }

  def trainModelDF( _dataFile : String )
    : org.apache.spark.ml.classification.NaiveBayesModel = {
    val weiNBC = new weiNaiveBayesModel(_dataFile,"",
      "NaiveBayesExample","local")
    weiNBC.getModel( true ).asInstanceOf[org.apache.spark.ml.classification.NaiveBayesModel]
  }

  def main(args : Array[String]) = {
    //    val model = trainModelRDD(
    //      "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\ModelExport\\data\\extract_combined_data.rst")
    //    model2File(model,"D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\ModelExport\\data\\123.rst")
        val model = trainModelDF(
          "extract_combined_data.rst")
        model2File(model,"model.export")
  }

  def model2File( _model : org.apache.spark.ml.classification.NaiveBayesModel, _out : String) = {
    val w = new java.io.PrintWriter(_out)
    var rst = ""
    rst += "model_type " + _model.getModelType + "\n"
    rst += "class_count " + _model.numClasses.toString + "\n"
    rst += "labels " + "0 1\n"///
    rst += "pi " + _model.pi.toArray.mkString(" ") + "\n"
    rst += "lamda " + _model.getSmoothing + "\n"
    rst += "features_count " + _model.numFeatures.toString + "\n"
    w.write(rst)
    rst = ""
    rst += "theta : \n"
    w.write(rst)
    rst = ""
    w.flush
    rst = _model.theta.transpose.rowIter.map(_.toArray.mkString(" ")).mkString("\n")
    w.write(rst)
    rst = ""
    w.flush
    w.close
  }

  def model2File( _model : org.apache.spark.mllib.classification.NaiveBayesModel, _out : String) = {
    val w = new java.io.PrintWriter(_out)
    var rst = ""
    rst += "model_type " + _model.modelType + "\n"
    rst += "class_count " + _model.labels.size.toString + "\n"
    rst += "labels " + _model.labels.mkString(" ") + "\n"
    rst += "pi " + _model.pi.mkString(" ") + "\n"
    rst += "features_count " + _model.theta.head.size.toString + "\n"
    w.write(rst)
    rst = ""
    rst += "theta : \n"
    w.write(rst)
    rst = ""
    w.flush
    val arr = _model.theta
    (0 until arr.head.size).indices.map {
      i =>
        (0 until arr.size).indices.map {
          j =>
            rst += arr(j)(i).toString + " "
        }
        rst += "\n"
    }
    w.write(rst)
    rst = ""
    w.flush
    w.close
  }
}
