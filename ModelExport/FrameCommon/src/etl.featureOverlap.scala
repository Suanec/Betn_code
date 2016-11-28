import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast

import com.weibo.datasys.Common._
import com.weibo.datasys.Common.ConfSpecs._
import com.weibo.datasys.Common.DataMappor.dataCleansing
import com.weibo.datasys.Common.ParseConfFiles._

import com.weibo.datasys.Macros.MacroRunnable
import scala.reflect.runtime._

object featureOverlap extends Serializable {
    
  type featureType = Array[Double]
  type featurePairType = (String,featureType)
  type dataType = Array[featurePairType]

  @transient
  def overlapFeatures(sparkSession : SparkSession,
    dataConf : String,
    featureConf : String,
    dataPath : String,
    overlapList : String,
    nameDelimiter : String = "+",
    fieldDelimiter : String = ",") : RDD[Option[dataType]] = {
    val dcc /*: DataConfType*/ = ParseConfFiles.loadDataConf(dataConf)
    val fmc /*: FeatureConfType */ = ParseConfFiles.loadFeatureConf(featureConf)
    val idxs : Array[(String,Int)] = ParseConfFiles.getColsID(dcc,fmc)
    val cleanedData : RDD[Array[(String,String)]] = sparkSession.sparkContext.textFile(dataPath).map(x => dataCleansing(idxs,x))
    val fmcB : Broadcast[FeatureMapType] = sparkSession.sparkContext.broadcast(fmc)
    val idxsB : Broadcast[Array[(String,Int)]] = sparkSession.sparkContext.broadcast(idxs)
    cleanedData.map{
      line => 
        val featureMapporRst = featurePairsMappor(fmcB.value,line)
        overlapFeaturePairs(featureMapporRst,overlapList,nameDelimiter,fieldDelimiter)
    }
  }
  
  // overlapMultiFeaturePair(t,l).get.last._2.size

  /// feature pair overlap by source data + feature name list 
  /// if some name can not be found in featureArray, return None
  @transient
  def overlapFeaturePairs(features : dataType,
    featureList : String,
    nameDelimiter : String = "+",
    fieldDelimiter : String = ",") : Option[dataType] = {
    if(nameDelimiter.size != 1) {
      println(s"nameDelimiter format error! nameDelimiter : ${nameDelimiter}")
      return None
    }
    if(fieldDelimiter.size != 1) {
      println(s"fieldDelimiter format error! fieldDelimiter : ${fieldDelimiter}")
      return None
    }
    val overlapPattern : scala.util.matching.Regex = """\[(.*)\]""".r 
    val overlapPattern(overlapString) : String = featureList
    val overlapList : Array[String] = overlapString.split(fieldDelimiter.head)
    val nameList : Array[Array[String]] = overlapList.map(_.split(nameDelimiter.head))
    val rst : Array[Option[dataType]] = nameList.map{
      list => 
        list.size match {
          case 2 => overlapTwoFeaturePair(features,list.head,list.last)
          case 1 => {
            println(s"only one feature name given, return source data without overlap modified!")
            Some(Array.empty[(String,featureType)])
          }
          case 0 => {
            println(s"no feature name given, return source data without overlap modified!")
            Some(Array.empty[(String,featureType)])
          }
          case _ => overlapMultiFeaturePair(features,list,nameDelimiter.head)
        }
    }
    if( rst.size != rst.filter(x => x != None).size) return None
    val rstValue : dataType = rst.map(_.get).reduce(_ ++ _)
    Some(features ++ rstValue)
  }

  /// feature multipairs overlap 
  /// give a list of feature name, return the overlapped data.
  /// if the name list size less of 2, return None.
  /// if some name can not be found in featureArray, return None
  @transient
  def overlapMultiFeaturePair( features : dataType,
    featureNameList : Array[String],
    nameDelimiter : Char = '+') : Option[dataType] = {
    if(featureNameList.size < 2) return None
    var rstValue : featureType = Array.fill(1)(1d)
    val mapFeatures : Map[String,featureType] = features.toMap
    val notFoundName : Array[String] = featureNameList.filter(mapFeatures.get(_) == None)
    if(notFoundName.size != 0) {
      println(s"Some feature name can not be found in source data : ")
      notFoundName.foreach(println)
      return None
    }
    featureNameList.indices.map{
      i => 
        val content : Array[Double]= mapFeatures.get(featureNameList(i)).get
          rstValue = overlapCross(rstValue,content).get
    }
    val name : String = featureNameList.reduce(
      (x,y) => 
        x + nameDelimiter.toString + y)
    val keyValue : (String,Array[Double]) = name -> rstValue.toArray
    Some(Array(keyValue))
  }

  /// feature pairs overlap
  /// give source data and two feature names, return the overlapped data. not include source data
  /// if features'name not found return None.
  @transient
  def overlapTwoFeaturePair( features : dataType,
    featureA : String,
    featureB : String,
    nameDelimiter : Char = '+' ) : Option[dataType] = {
    val overlappedFeature : Option[featureType] = overlapTwoFeature(
      features,featureA,featureB)
    overlappedFeature match {
      case None => return None
      case Some(_) => {
        val overlappedName : String = featureA + nameDelimiter + featureB
        val overlappedPair : (String,featureType) = 
        overlappedName -> overlappedFeature.get
        Some(Array(overlappedPair))
      }/// case Some(_)
    }/// match
  }

  /// feature overlap 
  /// give source data and two feature names, return feature overlap result
  /// if featureName not found return None.
  /// source data is after DataMappor
  @transient
  def overlapTwoFeature( features : dataType,
    featureA : String,
    featureB : String) : Option[featureType] = {
    val mapFeatures : Map[String,featureType] = features.toMap
    if( mapFeatures.get(featureA) == None || 
        mapFeatures.get(featureB) == None ) return None 
    val contentA : featureType = mapFeatures.get(featureA).get
    val contentB : featureType = mapFeatures.get(featureB).get
    overlapCross(contentA,contentB)
  }
  /// feature array overlap 
  /// give two array of double cross them to impletements feature overlap
  @transient
  def overlapCross(contentA : featureType,
    contentB : featureType) : Option[featureType] = {
    var rstValue : List[Double] = List.empty[Double]
    contentA.map{
      x => 
        x match {
          case 0 => rstValue ++= List.fill(contentB.size)(0d)
          case 1 => rstValue ++= contentB
          case _ => {
            println(s"some error had been found in datafeature : ${x}.")
            return None
          }
        }
    }
    Some(rstValue.toArray)
  }

  def dataCleansing(_idxs : Array[(String,Int)], _str : String ) : Array[(String,String)] = {
      val kdata_line = _str.split('\t')
      val key_data_line : Array[(String,String)] = _idxs.map{
        nameIdx =>
          var value = ""
          value = nameIdx._1 match {
            case name if name.contains("gender") =>
              kdata_line(nameIdx._2) match {
              case "m" => "1"
              case "f" => "0"
              case _ => "2"
            }
            case _ => kdata_line(nameIdx._2) match {
              case "\\N" => 0.toString
              case _ => kdata_line(nameIdx._2)
            }
          }
          nameIdx._1 -> value
      }
      key_data_line
    }

  def featurePairsMappor(
                            _fmc : ConfSpecs.FeatureMapType,
                            _features : Array[(String,String)]) : Array[(String,Array[Double])] = {
      val featuresValue = _features.map{
        featurePair =>
          val featureConf:(FeatureConf, Array[FeatureContent]) = _fmc.get(featurePair._1).get
          val dataDim = featureConf._2.size
          val data = Array.fill[Double](dataDim)(0)
          val featureCategory = featureConf._1._category
          val featureFormula = Option(featureConf._1._operation).getOrElse("Log10")
          featureCategory match {
            case "bool" => {
              val subIdx = featurePair._2.toInt
              (subIdx >= 0 && subIdx <= 1) match {
                case true => data(subIdx) = 1
                case false => data(data.size-1) = 1
              }
            }
            case "enum" => {
              val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
              val mathFormula = runtimeMirror.reflectModule(runtimeMirror.staticModule("com.weibo.datasys.Macros." + featureFormula.capitalize)).
                instance.asInstanceOf[MacroRunnable]

              val subIdx = mathFormula.run(featurePair._2).toDouble.toInt
              (subIdx >= 0 && subIdx < dataDim) match {
                case true => data(subIdx) = 1
                case false => data(dataDim - 1) = 1
              }
            }
            case "origin" => { data(0) = featurePair._2.toDouble }
            case _ => {/**featureCategory match case _ **/ }
          }
          featurePair._1 -> data
      }
      featuresValue
    }
}

object overlapTest{
  // val path = "/user/hadoop/testSpark/"
  // val dcf = "/home/hadoop/suanec/betn_code/data/dcf"
  val wsp = "/home/hadoop/suanec/betn_code/data"
  // val de = getDataElems(dcf)
  // val dst = genStructField(de).init
  val dataPath = "file://" + wsp + "/data.sample"
  val dataConf = wsp + "/data.conf"
  val featureConf = wsp + "/feature.conf"

  val overlapName = "[m_is_original+u_fans,m_has_pic+m_is_toutiao]"

  featureOverlap.overlapFeatures(spark,dataConf,featureConf,dataPath,overlapName)
}


