// package weiSpark.ml.classification.NaiveBayes
package com.weibo.datasys.weispark.ml.NaiveBayes

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

object RDDConfParser extends Serializable {
  val seperator = '\t'
  @transient
  def loadDataConf( _path : String, _codec : String = "UTF-8" ) : ConfUtil.DataConfType = {
    val contents = ConfFileHelper.readFile(_path,_codec)
      .toArray
      .filterNot(x => x.startsWith("@") ||
        x.startsWith("#") ||
        x.size < 1)
      .map(_.split('@'))
      .filterNot(_.size < 2)
      .map{
        splits => 
          splits.size match {
            case 2 => new ConfUtil.DataConf(splits(0).toInt,splits(1))
            case 3 => new ConfUtil.DataConf(splits(0).toInt,splits(1),splits(2))
            case 4 => new ConfUtil.DataConf(splits(0).toInt,splits(1),splits(2),splits(3))
            case _ => {
              println("error when read data.conf,format not match!!" + 
                s"${splits.size}")
              new ConfUtil.DataConf(-1,"","")
            }
          }
      } 
      .map{
        case(dcf) => dcf._name -> dcf 
      }.toMap
      contents
  }
  @transient
  def loadFeatureMap( _path : String, 
    _codec : String = "UTF-8" ) : ConfUtil.FeatureMapType = {
    ConfFileHelper.readFile(_path,_codec)
      .toArray
      .filterNot( x =>  x.startsWith("@") ||
        (x.split(seperator).size < 2 && x.split(seperator).size < 2))
      .mkString("\n")
      .split('#')
      .map(_.split('\n'))
      .filterNot(_.size < 2)
      .map{
        featureAndContent =>
          val featureSplits = featureAndContent.head.split(seperator).size match {
            case 1 => featureAndContent.head.split(seperator)
            case _ => featureAndContent.head.split(seperator)
          }
          val contentSplits = featureAndContent.tail.map(_.split(seperator).size).max  match {
            case 1 => featureAndContent.tail.map(_.split(seperator))
            case _ => featureAndContent.tail.map(_.split(seperator))
          }
          val featureConf = featureSplits.size match {
            case 2 => new ConfUtil.FeatureConf(featureSplits(0),featureSplits(1))
            case 3 => new ConfUtil.FeatureConf(featureSplits(0),featureSplits(1),featureSplits(2))
            case _ => {
              println("error when read feature.map,format not match!!" + 
              s"error with featureConf.size ${featureSplits.size}")
              new ConfUtil.FeatureConf("","")
            }/// match case _
          }/// match 
          val featureContent = contentSplits.map{
            featureContentLine =>
              // println(featureContentLine.head)
              featureContentLine.size match {
                case 2 => contentSplits(1).size.equals("_other_") match {
                  case true => new ConfUtil.FeatureContent(
                    featureContentLine(0).toInt,
                    featureContentLine(1).toInt,0,
                    "")
                  case false => new ConfUtil.FeatureContent(
                    featureContentLine(0).toInt,
                    featureContentLine.size -1, 0,
                    featureContentLine(1))
                }
                case 3 => new ConfUtil.FeatureContent(
                  featureContentLine(0).toInt,
                  featureContentLine(1).toInt,
                  featureContentLine(2).toInt)
                case 4 => new ConfUtil.FeatureContent(
                  featureContentLine(0).toInt,
                  featureContentLine(1).toInt,
                  featureContentLine(2).toInt,
                  featureContentLine(3))
                case _ => {
                  println("error when read feature.map,format not match!!" + 
                  s"error with featureContentLine.size : ${featureContentLine.size}")
                  new ConfUtil.FeatureContent(-1,-1)
                }/// match case
              }/// match 
          }/// contentSplits.map 
          // .toMap
          featureConf -> featureContent
      }/// filterNot.map 
      .map( x => x._1._name -> x )
      .toMap
  }
  @transient
  def getColsID( _dcc : ConfUtil.DataConfType, 
    _fmc : ConfUtil.FeatureMapType,
    _isLabeled : Boolean = true, /// @Deprecated 
    _defaultLabel : Int = 0 /// @Deprecated 
    ) : Array[(String,Int)] = {
    val featureNames = _fmc.toArray.map(_._1)
    val rstIdxs = featureNames.map( x => _dcc.get(x).get._name -> _dcc.get(x).get._idx)
    val _isHasLabel = _dcc.get("label") match {
      case None => false
      case Some(_) => true
    }
    (_isLabeled && _isHasLabel) match {
      case true => ((_dcc.get("label").get._name -> _dcc.get("label").get._idx) +: rstIdxs ).sortBy{x => x._2}
      case false => rstIdxs.sortBy{_._2}
    }
  }
  @transient
  def readMaxColNum( _fmc : ConfUtil.FeatureMapType ) : Int = _fmc.map(_._2._2.map(_._idx)).flatten.max 
}

