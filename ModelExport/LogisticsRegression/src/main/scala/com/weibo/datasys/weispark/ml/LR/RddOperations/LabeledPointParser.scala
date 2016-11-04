  // val path = """D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\LogisticsRegression\rst\123"""
  // val data = sc.textFile(path)//.map()
  
object LabeledPointParser {
  def lbpParser( _str : String ) : org.apache.spark.mllib.regression.LabeledPoint = {
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.linalg.Vectors
    _str.split('[').size match {
      case 2 => {
        val pattern = """\((.{0,}),\[(.{0,})\]\)""".r 
        val pattern(labelt,valuet) = _str
        val label = labelt.toDouble
        val value = valuet.split(',').map(_.toDouble)
        new LabeledPoint(label,Vectors.dense(value))
      }
      case 3 => {
        val pattern = """\((.{0,}),\((\d{0,}),\[(.{0,})\],\[(.{0,})\]\)\)""".r 
        val pattern(labelt,sizet,idxt,valuet) = _str
        val label = labelt.toDouble
        val size = sizet.toInt
        val idx = idxt.split(',').map(_.toInt)
        val value = valuet.split(',').map(_.toDouble)
        new LabeledPoint(label,Vectors.sparse(size,idx,value))
      }
      case _ => {
        println(s"source data error with ${_str}")
        new LabeledPoint(0,Vectors.dense(Array(0d)))
      }
    }
  }
}