package com.weibo.datasys.weispark.ml.LogisticsRegression.RDDOperations
/// @author : suanec_Betn
/// @version : 0.2
/// @data : 2016/10/24

object ConfUtil extends Serializable {

  case class DataConf(
    _idx : Int,
    _name : String,
    _category : String = "",
    _operation : String = "") extends Serializable

  case class FeatureConf(
    _name : String,
    _category : String,
    _operation : String = "") extends Serializable

  case class FeatureContent(
    _idx : Int,
    _subIdx : Int,
    _default : Int = 0,
    _info : String = "") extends Serializable

  type DataConfType = Map[String,ConfUtil.DataConf]

  type FeatureMapType = Map[String,(ConfUtil.FeatureConf,Array[ConfUtil.FeatureContent])]

}

object ConfFileHelper extends Serializable  {

  def testFile(_f : String) = new java.io.File(_f).isFile

  def readFile(_f : String) = scala.io.Source.fromFile(_f).getLines

  def readFile(_f : String, _codec : String) = scala.io.Source.fromFile(_f)(_codec).getLines

}
