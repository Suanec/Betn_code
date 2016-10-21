object ConfParser {
    
  case class DataConf(
    _idx : Int,
    _name : String,
    _category : String = "",
    _operation : String = "")

  val seperator = '\t'
  def loadDataConf( _path : String, _codec : String = "UTF-8" ) : Map[String,DataConf] = {
    val contents = colsOperate.readFile(_path,_codec)
      .toArray
      .filterNot(x => x.startsWith("@") ||
        x.startsWith("#") ||
        x.size < 1)
      .map(_.split('@'))
      .filterNot(_.size < 2)
      .map{
        splits => 
          splits.size match {
            case 2 => new DataConf(splits(0).toInt,splits(1))
            case 3 => new DataConf(splits(0).toInt,splits(1),splits(2))
            case 4 => new DataConf(splits(0).toInt,splits(1),splits(2),splits(3))
            case _ => {
              println("error when read data.conf,format not match!!" + 
                s"${splits.size}")
              new DataConf(-1,"","")
            }
          }
      } 
      .map{
        case(dcf) => dcf._name -> dcf 
      }.toMap
      contents
  }

  case class FeatureConf(
    _name : String,
    _category : String,
    _operation : String = "")

  case class FeatureContent(
    _idx : Int,
    _subIdx : Int,
    _default : Int = 0,
    _info : String = "")

  def loadFeatureMap( _path : String, _codec : String = "UTF-8" ) : Map[String,(FeatureConf,Array[FeatureContent])] = {
    colsOperate.readFile(_path,_codec)
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
            case 2 => new FeatureConf(featureSplits(0),featureSplits(1))
            case 3 => new FeatureConf(featureSplits(0),featureSplits(1),featureSplits(2))
            case _ => {
              println("error when read feature.map,format not match!!" + 
              s"error with featureConf.size ${featureSplits.size}")
              new FeatureConf("","")
            }/// match case _
          }/// match 
          val featureContent = contentSplits.map{
            featureContentLine =>
              // println(featureContentLine.head)
              featureContentLine.size match {
                case 2 => contentSplits(1).size.equals("_other_") match {
                  case true => new FeatureContent(
                    featureContentLine(0).toInt,
                    featureContentLine(1).toInt,0,
                    "")
                  case false => new FeatureContent(
                    featureContentLine(0).toInt,
                    featureContentLine.size -1, 0,
                    featureContentLine(1))
                }
                case 3 => new FeatureContent(
                  featureContentLine(0).toInt,
                  featureContentLine(1).toInt,
                  featureContentLine(2).toInt)
                case 4 => new FeatureContent(
                  featureContentLine(0).toInt,
                  featureContentLine(1).toInt,
                  featureContentLine(2).toInt,
                  featureContentLine(3))
                case _ => {
                  println("error when read feature.map,format not match!!" + 
                  s"error with featureContentLine.size : ${featureContentLine.size}")
                  new FeatureContent(-1,-1)
                }/// match case
              }/// match 
          }/// contentSplits.map 
          // .toMap
          featureConf -> featureContent
      }/// filterNot.map 
      .map( x => x._1._name -> x )
      .toMap
  }

  def getColsID( _dcc : Map[String,DataConf], 
    _fmc : Map[String,(FeatureConf,Array[FeatureContent])],
    _isLabeld : Boolean = true,
    _defaultLabel : Int = 0
    ) : Array[(String,Int)] = {
    val featureNames = _fmc.toArray.map(_._1)
    val rstIdxs = featureNames.map( x => _dcc.get(x).get._name -> _dcc.get(x).get._idx)
    _isLabeld match {
      case true => ((_dcc.get("label").get._name -> _dcc.get("label").get._idx) +: rstIdxs ).sortBy{x => x._2}
      case false => rstIdxs.sortBy{_._2}
    }
  }

  def readMaxColNum( _fmc : Map[String,(FeatureConf,Array[FeatureContent])] ) : Int = _fmc.map(_._2._2.map(_._idx)).flatten.max 
}

