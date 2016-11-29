import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.{Configuration => hdfsConfig}
import org.apache.spark.rdd.RDD
// import org.apache.hadoop.
import java.net.URI


object HdfsHelper {

  // val path = "/user/hadoop/testSpark/"
  val wsp = "/home/hadoop/suanec/betn_code/data"
  val dataPath = "file://" + wsp + "/data.sample"
  val dataConf = wsp + "/data.conf"
  val featureConf = wsp + "/feature.conf"


  /// check if the path exists on hdfs 
  def hdfsExists(_path : String) : Boolean = {
    val hdfs = FileSystem.get( new URI("/"),new hdfsConfig )
    hdfs.exists(new Path(_path))
  }

  def hdfsDelete(_path : String) : Boolean = {
    val hdfs = FileSystem.get( new URI("/"),new hdfsConfig )
    hdfs.delete(new Path(_path), true)
  }

  def hdfsListFiles( _path : String ) : Array[String] = {
    val hdfs = FileSystem.get( new URI("/"),new hdfsConfig )
    hdfs.listStatus(new Path(_path)).map(_.getPath.toString)    
  }

  def hdfsIsFile( _path : String ) : Boolean = {
    FileSystem.get(new URI("/"), new hdfsConfig).isFile(new Path(_path))
  }

  def hdfsIsDir( _path : String ) : Boolean = {
    FileSystem.get(new URI("/"), new hdfsConfig).isDirectory(new Path(_path))
  }

  def showListFiles( _path : String ) : Unit = {
    println(hdfsListFiles(_path).map{
      x => 
        val predix = x + "\t====>\t"
        // val fileType = hdfsIsDir(x) match {
        //   case true => "Dir." 
        //   case false => "File."
        // }
        val fileType = if(hdfsIsDir(x)) "Dir." else "File."
        predix + fileType
    } .mkString("\n"))
  }

  def hdfsRename( _srcName : String , _targetName : String ) : Boolean = {
    FileSystem.get(new URI("/"), new hdfsConfig).rename(new Path(_srcName), new Path(_targetName))
  }

  def hdfsSaveAsTextFile( _data : RDD[String], _out : String ) : Boolean = {
    try { 
      _data.saveAsTextFile(_out)
      true
    } catch {
      case e : org.apache.hadoop.mapred.FileAlreadyExistsException => {
        println(e.getMessage)
        System.exit(1)
        false
      }
      case e : Throwable => {
        println(e.getMessage)
        false 
      }
    } 
  }

  def hdfsForceSaveAsTextFile( _data : RDD[String], _out : String ) = {
    try {
      hdfsExists(_out) match {
        case true => {
          hdfsDelete(_out)
          _data.saveAsTextFile(_out)
        }
        case false => _data.saveAsTextFile(_out)
      }
    } catch {
      case e : Throwable => println(e.getMessage)
    }
  }
  /// logic implements Naive
  /// loadPathStr Naive implements for read a pathList to a RDD
  private def loadPathStr(_sc : org.apache.spark.SparkContext, _pathsStr : String, _format : String = "text" ) = {
    val datas = _format match {
      case "text" => _pathsStr.split(',').map( s => _sc.textFile(s) )
      case _ => {
        println(s"format ${_format} not supported yet!!")
        Array(_sc.parallelize(Seq("")))
      }
    }
    datas.reduce( (x,y) => x.union(y) )
  }

  /// a little more robust implements for check the path is a second-level directory.
  /// loadPath for read a pathList to a RDD
  def loadPathRDD(_sc : org.apache.spark.SparkContext, 
    _path : String, _format : String = "text" ) : RDD[String] = {
    if(_path.size == 0) {
      println(s"input path can not be empty!!")
      _sc.parallelize(Seq(""))
    } else {
      val isList = _path.contains(",")
      isList match {
        case true => loadPathStr(_sc,_path,_format)
        case false => {
          if(_path.last == '*') 
            loadPathStr(_sc,_path,_format)
          else{
            val listFiles = hdfsListFiles(_path)
            val _pathStr = listFiles.find( _.toString.contains("_SUCCESS")) match {
              case Some(_) => _path
              case None => listFiles.mkString(",")
            }/// match 
            loadPathStr(_sc,_pathStr,_format)
          }/// else
        }/// case false 
      }/// isList match 
    }/// _path.size == 0 else
  }

  def loadPathRDDBySparkSession(_spark : org.apache.spark.sql.SparkSession, _path : String, _format : String = "text") : RDD[String] = {
    val _sc = _spark.sparkContext
    loadPathRDD(_sc,_path,_format)
  }

  def loadPath(_spark : Any, _path : String, _format : String = "text") : RDD[String] = {
    _spark match {
      case org.apache.spark.SparkContext => loadPathRDD(_spark.asInstanceOf[org.apache.spark.SparkContext],_path,_format)
      case org.apache.spark.sql.SparkSession => loadPathRDDBySparkSession(_spark.asInstanceOf[org.apache.spark.sql.SparkSession],_path,_format)
    }
  }

  // def LoadPaths(_spark : SparkSession, _pathStr : String, _format : String = "text" ) = {
  //   val datas = _pathStr.split(',').map{
  //     s => 
  //       _spark.read.format(_format).load(s)
  //   }
  //   val rst = datas.reduce( df => sc.read.format(_format).load )
  // }
  
}