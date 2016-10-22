package weiSpark.ml.classification.NaiveBayes

// import LibSVMUtil._


object weiNaiveBayesModel {

  case class weiNaiveBayesModel( modelType : String,
    labels : Array[Double],
    pi : Array[Double],
    theta : org.apache.spark.mllib.linalg.DenseMatrix) extends Serializable {
    def predict( _test : Util.WeiLabeledPoint ) : Double = {
      labels(theta.transpose.multiply(_test.features).argmax)
    }
    def predict( _testArr : Array[Util.WeiLabeledPoint] ) : Array[Double] = {
      _testArr.map(predict)
    }
    def validACC( _testArr : Array[Util.WeiLabeledPoint] ) : Double = {
      _testArr.filter( x => x.label == predict(x) ).size.toDouble / _testArr.size
    }
  }

  def readModel( _modelFile : String ) : weiNaiveBayesModel = {
    val iter = scala.io.Source.fromFile(_modelFile).getLines
    val modelType = iter.next.split(' ').last
    val classCount = iter.next.split(' ').last.toInt
    val labels = iter.next.split(' ').tail.map(_.toDouble)
    val pi = iter.next.split(' ').tail.map(_.toDouble)
    val featuresCount = iter.next.split(' ').last.toInt
    iter.next
    val wArr = iter.toArray.map(_.split(' ')).flatten.map(_.toDouble)
    val theta = new org.apache.spark.mllib.linalg.DenseMatrix(classCount,featuresCount,wArr).transpose
    val weiNBC = new weiNaiveBayesModel(modelType,labels,pi,theta)
    weiNBC
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
}