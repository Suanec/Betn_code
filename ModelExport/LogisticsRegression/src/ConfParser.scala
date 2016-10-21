object ConfParser {
    
  case class DataConf(
    _idx : Int,
    _name : String,
    _category : String,
    _operation : String = "")

  def loadDataConf( _path : String, _codec : String = "UTF-8" ) : Map[String,DataConf] = {
    val contents = colsOperate.readFile(_path,_codec)
      .toArray
      .filterNot(x => x.startsWith("@") ||
        x.startsWith("#") ||
        x.size < 1)
      .map(_.split('@'))
      .filterNot(_.size < 3)
      .map{
        splits => 
          splits.size match {
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

  def loadFeatureMap

}