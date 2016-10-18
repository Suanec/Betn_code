package weiSpark.ml.classification.NaiveBayes

object SparkHelper{
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
  import org.apache.spark.mllib.util.MLUtils

  val conf = new SparkConf().setAppName("NaiveBayesExample")
  val sc = new SparkContext(conf)

  def getNBCAcc( _in : String, _trainRate : Array[Double] ) = {
    import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
    import org.apache.spark.mllib.util.MLUtils
    val data = MLUtils.loadLibSVMFile(sc, _in)

    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(_trainRate)

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy : " + ( ( accuracy * 10000 ).toInt/100 ).toString )
    (model,accuracy)
    /// use exC Acc = 99%
  }
  def trainModel( _in : org.apache.spark.rdd.RDD[
    org.apache.spark.mllib.regression.LabeledPoint], 
    _trainRate : Array[Double] ) = {
    val Array(training,test) = _in.randomSplit(_trainRate)
    val model = NaiveBayes.train(training, lambda = 1d, modelType = "multinomial")
    model 
  }
  def modelPredict( _m :  org.apache.spark.mllib.classification.NaiveBayesModel, 
    _test : org.apache.spark.rdd.RDD[
    org.apache.spark.mllib.regression.LabeledPoint] ) = {
    val predictionAndLabel = _test.map(p => (_m.predict(p.features), p.label))
    1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / _test.count()
  }
  def genModel( _in : org.apache.spark.rdd.RDD[
    org.apache.spark.mllib.regression.LabeledPoint],
    _trainRate : Array[Double] = Array(0.8,0.2) ) = {
    val Array(training, test) = _in.randomSplit(_trainRate)
    val model = NaiveBayes.train(training, lambda = 1d, modelType = "multinomial")
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count/test.count
    accuracy
  }
  def findLimit( _data : org.apache.spark.rdd.RDD[
    org.apache.spark.mllib.regression.LabeledPoint], 
    _sampleRate : Double,
    _trainRate : Array[Double] = Array(0.8,0.2)) = {
    val k = _data.sample(true, _sampleRate)
    // val acc = genModel(k,_trainRate)
    val model = NaiveBayes.train(k, lambda = 1d, modelType = "multinomial")
    val acc = modelPredict(model, _data)
    println("Acc : " + acc)
    println("sampleRate : " + _sampleRate + ", sampleCount : " + k.count)
  }
  // 1 : 0.5
  // 11 : 0.84621
  // 87 : 0.98.089
  // 976 : 0.99783
  // 10046 : 0.99804
  // 99702 : 0.99811
}