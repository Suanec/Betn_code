package weiSpark.ml.classification.NaiveBayes


object Util{
  case class WeiLabeledPoint(
    label : Double,
    features : org.apache.spark.mllib.linalg.Vector) extends Serializable
}

