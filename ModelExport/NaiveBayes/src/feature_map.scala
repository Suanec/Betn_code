object feature_map {
  import colsOperate._
  import readLibSVM._
  import generateLibSVM._
  val path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\NaiveBayesModelExport\\NaiveBayes\\data"
  val file = path + "\\feature.map"
  val iter = colsOperate.readFile(file)
  val data = colsOperate.readFile(file).filterNot{
    line =>
      line.startsWith("#") || line.startsWith("@")
  }
  
  val map = data.map{
    line =>
      val splits = line.split('\t')
      // if(splits.size > 1)
      splits(0).toInt -> splits(1).toInt
  }.toMap
  
  def loadFeatureMap( _path : String ) : Map[Int,Int] = {
    colsOperate.readFile(file).filterNot{
      line =>
        line.startsWith("#") || line.startsWith("@")
    }.map{
      line =>
        val splits = line.split('\t')
        // if(splits.size > 1)
        splits(0).toInt -> (splits(1) match {
          case "_other_" => 1
          case _ => splits(1).toInt
        })
    }.toMap
  }
  /// name : String -> map(idx -> value)
  def loadFeatureMap( _path : String, _flag : Int ) : Map[String,Map[Int,Int]] = {
    colsOperate.readFile(file)
      .toArray
      .mkString("\n")
      .split('#')
      .filterNot( x => x.size < 3 || x.startsWith("@") )
      .map{
        feature_line =>
          val splits = feature_line.split('\n').filter(_.size > 1)
          (splits.size > 0) match {
            case true => {
              val headLine = splits.head.split('\t')
              val features = splits.tail.map{
                feature_line => 
                  val feature_content = feature_line.split('\t')
                  feature_content.head.toInt -> (feature_content(1) match {
                    case "_other_" => 1
                    case _ => feature_content(1).toInt
                  }
                )
              }.toMap
              headLine.head -> features
            }
            case false => "" -> new Array[(Int,Int)](0).toMap
          }
      }.filter(_._1.size > 0)
      .toMap
  }

}