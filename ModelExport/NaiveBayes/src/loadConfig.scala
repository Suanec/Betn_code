object loadConfig {
  def strConf2Pair(_str : String) : (String,String) = {
    val splits = _str.split('=')
    splits.head -> splits.last
  }
  def findProcess(_arrConf : Array[String]) : (Array[String],Array[String]) = {
    _arrConf.splitAt(
      _arrConf.indexOf(
        _arrConf.find(_.contains("FEATURE_PROCESSES")).get))
  }
  def readProcess(_arrProcesses : Array[String]) : Map[String,String] = {
      _arrProcesses.tail.init
      .map(_.trim)
      .mkString
      .split(',')
      .map(strConf2Pair)
      .toMap
  }
  def readPaths(_arrPaths : Array[String]) : Map[String,String] = {
    _arrPaths
      .filter(_.contains("="))
      .map(strConf2Pair)
      .toMap
  }
  def loadConf(_path : String) : Map[String,String] = {
    val arrConf = colsOperate.readFile(_path).toArray.filterNot(_.startsWith("#"))
    val (arrPaths,arrProcesses) = findProcess(arrConf)
    val mapPaths = readPaths(arrPaths)
    val mapProcesses = readProcess(arrProcesses)
    mapPaths ++ mapProcesses
  }
}