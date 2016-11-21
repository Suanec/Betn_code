object HdfsHelper {
  
  /// logic implements Naive
  /// loadPathStr Naive implements for read a pathList to a RDD
  private def loadPathStr(_sc : org.apache.spark.SparkContext, _pathsStr : String, _format : String = "text" ) = {
    val datas = _format match {
      case "text" => _pathsStr.split(',').map( s => sc.textFile(s) )
      case _ => {
        println(s"format ${_format} not supported yet!!")
        Array(sc.parallelize(Seq("")))
      }
    }
    datas.reduce( (x,y) => x.union(y) )
  }

  /// a little more robust implements for check the path is a second-level directory.
  /// loadPath for read a pathList to a RDD
  def loadPath(_sc : org.apache.spark.SparkContext, _path : String, _format : String = "text" ) = {
    val isList = _path.contains(",")
    isList match {
      case true => loadPathStr(_sc,_path,_format)
      case false => {
        val path = new java.io.File(_path)
        val _pathStr = path.listFiles.find( _.toString.contains("_SUCCESS")) match {
          case Some(_) => _path
          case None => path.listFiles.map(_.toString).mkString(",")
        }
        loadPathStr(_sc,_pathStr,_format)
      }
    }
  }

  def LoadPaths(_spark : SparkSession, _pathStr : String, _format : String = "text" ) = {
    val datas = _pathStr.split(',').map{
      s => 
        _spark.read.format(_format).load(s)
    }
    // val rst = datas.reduce( df => sc.read.format(_format).load )
  }
  
}