///author := "suanec_Betn"
///data := 20160918

package weiSpark.monitor
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.JsonDSL.map2jvalue
// or
//import org.json4s.JsonDSL.WithDouble._
//import org.json4s.JsonDSL.WithBigDecimal._
//import scala.io.Source
//import scala.collection.JavaConverters._
//import scala.collection.mutable.Buffer
import java.io.{File,PrintWriter}
import java.util.Date

object JsonHelper{
  implicit val formats = Serialization.formats(ShortTypeHints(List()))
  val confPath = """
    /data1/work/datasys/weispark/runtime/107716109/monitor/
    """
  val resPath = """
    /data1/work/datasys/weispark/runtime/107716109/monitor/
    """
  /// read Config from file name
  def readConf( _file : String = "" ) : String = {
    var file = _file
    if(_file.size == 0) file = confPath + "config.json"
    val conf = io.Source.fromFile(file)("utf-8").mkString
    conf
  }
  def getUiUrl( _conf : String ) : String = {
    val ui = parse(_conf) \ "ui"
    ui.extract[String]
  }
  /// find Ids' index
  def findIdIndex( _listArr : Array[String] ) : Array[Int] = {
    _listArr.indices.map{
      i =>
        if(_listArr(i).contains("application"))
          i
        else -1
    }.filter( _ >= 0 ).toArray
  }
  /// str arr to object arr through ids @Deprecated
  def arr2ObjArrByIds( _listArr : Array[String], 
    _idIdxArr : Array[Int] ) : Array[(String,Array[(String,String)])] = {
    val rst = new Array[(String,Array[(String,String)])](_idIdxArr.size)
    val featureArr = new Array[(String,String)](_idIdxArr(1)-_idIdxArr(0)-1)
    _idIdxArr.indices.map{
      i =>
        featureArr.indices.map{
          j =>
            val splits = _listArr(j+_idIdxArr(i)+1).split(':')
            featureArr(j) = (splits.head,splits.last)
        }
        rst(i) = (_listArr(_idIdxArr(i)),featureArr.clone)
    }
    rst
  }
  /// objects to Map @Deprecated
  def objsArr2Map( _objs : Array[(String,Array[(String,String)])] ) : Map[String,Map[String,String]] = {
    _objs.map{
      k =>
        (k._1,k._2.toMap)
    }.toMap
  }
  def arr2ObjByIds( _listArr : Array[String], 
    _idIdxArr : Array[Int] ) : Map[String,Map[String,String]] = {
    val rst = new Array[(String,Map[String,String])](_idIdxArr.size)
    val featureArr = new Array[(String,String)](_idIdxArr(1)-_idIdxArr(0)-1)
    _idIdxArr.indices.map{
      i =>
        featureArr.indices.map{
          j =>
            val splits = _listArr(j+_idIdxArr(i)+1).split(':')
            featureArr(j) = (splits.head,splits.last)
        }
        rst(i) = (_listArr(_idIdxArr(i)),featureArr.clone.toMap)
    }
    rst.toMap
  }
  /// str arr to object arr through ids
  def genDataArray( _listArr : Array[String]) : Array[(String,Array[String])] = {
    val ids = findIdIndex(_listArr)
    val rst = new Array[(String,Array[String])](ids.size)
    val featureArr = new Array[String](ids(1)-ids(0)-1)
    ids.indices.map{
      i =>
        featureArr.indices.map{
          j =>
            featureArr(j) = _listArr(j+ids(i)+1)
        }
        rst(i) = (_listArr(ids(i)),featureArr.clone)
    }
    rst
  }

  /// list data to Map, call functions objs' operation ; 
  /// _listStr is a string that all info one line 
  def list2Map( _listStr : String ) : Map[String,Map[String,String]] = {
    val arr = _listStr.split('\n').tail
    val ids = findIdIndex(arr)
    val rst = arr2ObjByIds( arr, ids )
    rst
  }
  /// list data to json
  /// _listStr is a string that all info one line 
  def list2Json( _listRst : Map[String,Map[String,String]], _conf : String ) : String = {
    val rst1 = parse(_conf)
    val rst2 = render(("ApplicationInfo",_listRst))
    val rst = pretty(render(rst1 merge rst2))
    rst
  } 
  /// grammar sugar
  def listStr2Json( _listStr : String, _conf : String ) : String = {
    val rst = pretty(
      render(
        parse(_conf) merge render(
          ( "ApplicationInfo",list2Map(_listStr) 
          )
        )
      )
    )
    rst
  }
  /// info to json
  /// info is a Array(applicationId,Array[appInfo])
  def info2Json0( _arrData : Array[(String,Array[String])], _conf : String ) : String = {
    val data = _arrData.map{
      k =>
        val kk = k._2.map{
          l =>
            val split = l.split(':')
            (split.head,split.last)
        }.toMap
      (k._1,kk)
    }.toMap
    val d = new Date(System.currentTimeMillis).toString
    val dd = "Date" -> d
    val rst2 = render(data.toList)
    val rst1 = dd ++ parse(_conf)
    pretty(render(rst1 merge rst2))
  }
  /// MonitorSparkUI rstArr to Map[String,String]
  def uiArr2JObject0( _arrData : Array[(String, Array[String])] ) : JValue = {
    val ids = _arrData.map(_._1)
    val objs = ids.indices.map{
      i =>
        val str = "JobId: " + ids(i)
        (Array(str) ++ _arrData(i)._2).map{
          k =>
            val kk = k.split(':')
            (kk.head,kk.last)
        }.toMap
    }
    var sumObj = render(objs.head)
    (1 until objs.size).map{
      i =>
        sumObj = sumObj ++ render(objs(i))
    }
    sumObj
  }
  def uiArr2JObject( _arrData : Array[(String, Array[String])] ) : JArray = {
    val ids = _arrData.map(_._1)
    val objs = ids.indices.map{
      i =>
        val str = "JobId: " + ids(i)
        (Array(str) ++ _arrData(i)._2).map{
          k =>
            val kk = k.split(':')
            (kk.head,kk.last)
        }.toMap
    }.map( render(_) ).toList
    JArray(objs)
  }
  def info2Json( _arrData : Array[(String,Array[String])], _conf : String ) : String = {
    val sumObj = uiArr2JObject(_arrData)
    val confObj = parse(_conf)
    pretty( ("conf",confObj) ~ ("ApplicationInfo",sumObj) )
  }
}  

