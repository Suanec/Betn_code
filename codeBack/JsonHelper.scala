///author := "suanec_Betn"
///data := 20160918
/***
application_1466682549428_0192==============================================
Total Duration: 1.7 h
Scheduling Mode: FIFO
Active Jobs: 2
Completed Jobs: 845
Started at: 1473838848670
Time since start: 1 hour 44 minutes 20 seconds
Network receivers: 1
Batch interval: 10 ms
Processed batches: 20267
Waiting batches: 1
Received records: 1542399
Processed records: 1540248
application_1466682549428_0193==============================================
Total Duration: 1.4 h
Scheduling Mode: FIFO
Active Jobs: 2
Completed Jobs: 878
Started at: 1473840147734
Time since start: 1 hour 23 minutes 8 seconds
Network receivers: 1
Batch interval: 10 ms
Processed batches: 26888
Waiting batches: 0
Received records: 1166547
Processed records: 1166547
application_1466682549428_0191==============================================
Total Duration: 1.8 h
Scheduling Mode: FIFO
Active Jobs: 2
Completed Jobs: 559
Started at: 1473838830102
Time since start: 1 hour 46 minutes 11 seconds
Network receivers: 1
Batch interval: 10 ms
Processed batches: 17012
Waiting batches: 1
Received records: 1368431
Processed records: 1368430
***/
/***
val data = """
application_1466682549428_0192
Total Duration: 1.7 h
Scheduling Mode: FIFO
Active Jobs: 2
Completed Jobs: 845
Started at: 1473838848670
Time since start: 1 hour 44 minutes 20 seconds
Network receivers: 1
Batch interval: 10 ms
Processed batches: 20267
Waiting batches: 1
Received records: 1542399
Processed records: 1540248
application_1466682549428_0193
Total Duration: 1.4 h
Scheduling Mode: FIFO
Active Jobs: 2
Completed Jobs: 878
Started at: 1473840147734
Time since start: 1 hour 23 minutes 8 seconds
Network receivers: 1
Batch interval: 10 ms
Processed batches: 26888
Waiting batches: 0
Received records: 1166547
Processed records: 1166547
application_1466682549428_0191
Total Duration: 1.8 h
Scheduling Mode: FIFO
Active Jobs: 2
Completed Jobs: 559
Started at: 1473838830102
Time since start: 1 hour 46 minutes 11 seconds
Network receivers: 1
Batch interval: 10 ms
Processed batches: 17012
Waiting batches: 1
Received records: 1368431
Processed records: 1368430
"""
***/
package weiSpark.monitor
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
// or
import org.json4s.JsonDSL.WithDouble._
import org.json4s.JsonDSL.WithBigDecimal._
import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import java.io.{File,PrintWriter}

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
  /// info to json
  /// info is a Array(applicationId,Array[appInfo])
  def info2Json( _arrData : Array[(String,Array[String])], _conf : String ) : String = {
    val data0 = _arrData.map{
      k =>
        val kk = k._2.map{
          l =>
            val split = l.split(':')
            (split.head,split.last)
        }.toMap
      (k._1,kk)
    }.toMap
    val data = data0.map

    val rst2 = render(data)
    val rst1 = parse(_conf)
    pretty(render(rst1 merge rst2))
  }


}  


/// A central concept in lift-json library is Json AST which models the structure of a JSON document as a syntax tree.

// sealed abstract class JValue
// case object JNothing extends JValue // 'zero' for JValue
// case object JNull extends JValue
// case class JString(s: String) extends JValue
// case class JDouble(num: Double) extends JValue
// case class JDecimal(num: BigDecimal) extends JValue
// case class JInt(num: BigInt) extends JValue
// case class JBool(value: Boolean) extends JValue
// case class JObject(obj: List[JField]) extends JValue
// case class JArray(arr: List[JValue]) extends JValue

// type JField = (String, JValue)

///========================Script===================================

// scala -cp ../../libs/json/json4sLib-assembly-1.0.jar
// import org.json4s._
// import org.json4s.native._
// import org.json4s._
// import org.json4s.native.JsonMethods._
// import org.json4s.JsonDSL._
// // or
// import org.json4s.JsonDSL.WithDouble._
// import org.json4s.JsonDSL.WithBigDecimal._
// implicit val formats = Serialization.formats(ShortTypeHints(List()))
// import scala.io.Source
// import java.io.File
// import scala.collection.JavaConverters._
// import scala.collection.mutable.Buffer
// val path = new File("").getAbsolutePath
// val file = new File(path + "\\conf\\config.json")
// val conf = io.Source.fromFile(file)("utf-8").mkString
// val jsonConf = parse(conf)
// val jsonRst = pretty(render(jsonConf))


//example
  // import org.json4s._
  // import org.json4s.JsonDSL._
  // import org.json4s.jackson.JsonMethods._

  // case class Winner(id: Long, numbers: List[Int])
  // case class Lotto(id: Long, winningNumbers: List[Int], winners: List[Winner], drawDate: Option[java.util.Date])

  // val winners = List(Winner(23, List(2, 45, 34, 23, 3, 5)), Winner(54, List(52, 3, 12, 11, 18, 22)))
  // val lotto = Lotto(5, List(2, 45, 34, 23, 7, 5, 3), winners, None)

  // val json =
  //   ("lotto" ->
  //     ("lotto-id" -> lotto.id) ~
  //     ("winning-numbers" -> lotto.winningNumbers) ~
  //     ("draw-date" -> lotto.drawDate.map(_.toString)) ~
  //     ("winners" ->
  //       lotto.winners.map { w =>
  //         (("winner-id" -> w.id) ~
  //          ("numbers" -> w.numbers))}))


// import org.json4s._
// import org.json4s.jackson.JsonMethods._