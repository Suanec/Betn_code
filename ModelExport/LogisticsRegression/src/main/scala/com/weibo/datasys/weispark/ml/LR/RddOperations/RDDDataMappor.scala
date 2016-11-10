package com.weibo.datasys.weispark.ml.LogisticsRegression.RDDOperations
/// @author : suanec_Betn
/// @version : 0.2
/// @data : 2016/10/24

 
object RDDDataMappor extends Serializable {

  def dataToLibsvm( 
    _strData : (String,String) 
    ) : String = dataToLibsvm(_strData._1,_strData._2)

  def dataToLibsvm( _label : String, _data : String ) : String = {
    if(_label.isEmpty ) return RDDgenerateLibSVM.vec2libsvm(_data.split(' ').map(_.toDouble))
    val isNegative = _label.trim.startsWith("-")
    val isNum = isNegative match {
      case false => _label.trim.map(c => c >= '0' && c <= '9').reduce( _&&_ )
      case true => _label.tail.trim.map(c => c >= '0' && c <= '9').reduce( _&&_ )
    }
    isNum match {
      case true => RDDgenerateLibSVM.vec2libsvm(_label.toInt,_data.split(' ').map(_.toDouble))
      case false => RDDgenerateLibSVM.vec2libsvm(_data.split(' ').map(_.toDouble))
    }
  }

  def dataDenoise(_idxs : Array[(String,Int)], _str : String ) : Array[(String,String)] = {
    val kdata_line = _str.split('\t')
    val key_data_line = _idxs.map{
      x =>
        var x1 = ""
        x1 = x._1 match {
          case "u_gender" => kdata_line(x._2) match {
            case "m" => "1"
            case "f" => "0"
            case _ => "2"
          }
          case _ => kdata_line(x._2) match {
            case "\\N" => (Int.MaxValue-1000).toString
            case _ => kdata_line(x._2)
          }
        }
        x._1 -> x1
    }/// Array((Key,value)) sortedBy colNum    
    key_data_line
  }
  def getLabelAndFeatures(_key_data_line : Array[(String,String)]) = {
    val label = _key_data_line.head._1 match {
      case "label" => _key_data_line.head._2
      case _ => ""
    }
    val features = label.size match {
      case 0 => _key_data_line
      case _ => _key_data_line.tail
    }
    label -> features
  }
  def featurePairsMappor(
    _fmc : ConfUtil.FeatureMapType,
    _features : Array[(String,String)]) : Array[(String,Array[Double])] = {
    val featuresValue = _features.map{
      featurePair =>
        val featureConf = _fmc.get(featurePair._1).get
        val dataDim = featureConf._2.size
        val data = Array.fill[Double](dataDim)(0)
        val featureCategory = featureConf._1._category
        val featureFormula = featureConf._1._operation
        featureCategory match {
          case "bool" => {
            val subIdx = featurePair._2.toInt
            (subIdx >= 0 && subIdx <= 1) match {
              case true => data(subIdx) = 1
              case false => data(data.size-1) = 1
            }
          }/// case bool 
          case "enum" => {
            // println( featureConf, featureFormula )
            featureFormula match {
              case "log10" => {
                val subIdx = scala.math.log10(featurePair._2.toDouble).toInt
                (subIdx >= 0 && subIdx < dataDim) match {
                  case true => data(subIdx) = 1
                  case false => data(dataDim-1) = 1
                }/// subIdx match 
              }/// featureFormula match case "log10"
              case "max20" => {
                // if(dataDim != maxSize) println( "FeatureMapFile error or featureOperation error"
                  // + s"for dataDim is ${dataDim} with featureOperation is ${featureFormula}")
                val subIdx = (featurePair._2.toDouble).toInt
                (subIdx >= 0 && subIdx < dataDim) match {
                  case true => data(subIdx) = 1
                  case false => data(dataDim-1) = 1
                }/// subIdx match 
              }/// featureFormula match case "max20"
              case _ => {/// featureFormula match case _ 
                val subIdx = scala.math.log(featurePair._2.toDouble).toInt
                (subIdx >= 0 && subIdx < dataDim) match {
                  case true => data(subIdx) = 1
                  case false => data(dataDim-1) = 1
                }/// subIdx match 
              }/// featureFormula match case _
            }/// featureFormula match 
          }/// featureCategory match case enum 
          case "origin" => { data(0) = featurePair._2.toDouble } 
          case _ => {/**featureCategory match case _ **/ }
        }/// match case 
        featurePair._1 -> data
    }/// featuresValue
    featuresValue
  }
  def RDDSingleLineMappor( 
    _fmc : ConfUtil.FeatureMapType,
    _dcc : ConfUtil.DataConfType, 
    _idxs : Array[(String,Int)], 
    _str : String,
    _isDataContainLabel : Boolean = true,
    _label : Int = 0 ) : String = {
    val key_data_line = dataDenoise(_idxs,_str)
    val (label,features) = getLabelAndFeatures(key_data_line)
    val featuresValue = featurePairsMappor(_fmc,features) /// featuresValue
    /// rst_data : return label -> Array[(String,Array[(key,data)])]
    val rst_line = label -> featuresValue
    val sortedLine = rst_line._1 -> (rst_line._2.sortBy{
      x =>
        _fmc.get(x._1).get._2.maxBy(_._idx)._idx
    }.map(_._2).flatten.mkString(" "))/// sortedData : label + data : String + Array[String]
    val rstData = _isDataContainLabel match {
      case true => dataToLibsvm(sortedLine._1,sortedLine._2)
      case false => dataToLibsvm(_label.toString, sortedLine._2)
    }
    rstData
  }/// RDDSingleLineMappor
}
// RDDDataMappor.RDDSingleLineMappor(fmc,dcc,idxs,t)

