package 
///author := "suanec_Betn"
///data := 20160918
object readLibSVM {

  /// single line libsvm to Sparse Vector Format 
  def parseLibSVMRecord(line: String): (Double, Array[Int], Array[Double]) = {
    val items = line.split(' ')
    val label = items.head.toDouble
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip

    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    while (i < indicesLength) {
      val current = indices(i)
      require(current > previous, s"indices should be one-based and in ascending order;"
        + " found current=$current, previous=$previous; line=\"$line\"")
      previous = current
      i += 1
    }
    (label, indices.toArray, values.toArray)
  }

  // get the libsvm dimision(the max indices)
  def computeNumFeatures(_arr : Array[(Double, Array[Int], Array[Double])]): Int = {
    _arr.map { case (label, indices, values) =>
      indices.lastOption.getOrElse(0)
    }.reduce(math.max) + 1
  }
  def computeNumFeatures(_iter : Iterator[(Double, Array[Int], Array[Double])]): Int = {
    _iter.map { case (label, indices, values) =>
      indices.lastOption.getOrElse(0)
    }.reduce(math.max) + 1
  }

  def parseLibSVMFile( _path: String ): Array[(Double, Array[Int], Array[Double])] = {
    scala.io.Source.fromFile(_path).getLines
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLibSVMRecord)
      .toArray
  }

  def libsvm2Data( _path : String = "", numFeatures: Int = -1) : Array[(Double,Array[Double])] = {
    val parsed = parseLibSVMFile(_path)
    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      computeNumFeatures(parsed)
    }
    parsed.map { case (label, indices, values) =>
      val datas = new Array[Double](d)
      indices.indices.map( i => datas(indices(i)) = values(i) )
      label.toDouble -> datas
    }
  }  

  def readParsed( _file : String ) : Iterator[(Double, Array[Int], Array[Double])] = {
    scala.io.Source.fromFile(_file).getLines
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLibSVMRecord)
  }
  def read( _file : String = "", numFeatures : Int = -1 ) : Iterator[(Double,Array[Double])] = {
    val parsed = readParsed(_file)
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      computeNumFeatures(readParsed(_file))
    }
    parsed.map { case (label, indices, values) =>
      val datas = new Array[Double](d)
      indices.indices.map( i => datas(indices(i)) = values(i) )
      label.toDouble -> datas
    }
  }

}

object generateLibSVM /*extends Any*/{

  // one vector in Array[Double] type convert to libsvm, split by space 
  // features'indices start from 1
  def vec2libsvm( _vec : Array[Double] ) : String = {
    _vec
      .zipWithIndex
      .filterNot(_._1 == 0)
      .map{ 
        case( value, idx ) => 
        (idx + 1).toString + ":" + value.toString
      }
      .mkString(" ")
  }
  // generate LabeledPoint in libsvm format by data String 
  def vec2libsvm( _label : Int, _vec : Array[Double] ) : String = _label.toString + " " + vec2libsvm(_vec)

  // vector by DenseData convert to Array[String] by libsvm format
  def data2libsvm( _arr : Array[Array[Double]] ) : Array[String] = _arr.map(vec2libsvm)
  // convert data strings to LabeledPoint in libsvm, get an Array 
  def data2libsvm( _label : Int, _arr : Array[Array[Double]] ) : Array[String] = _arr.map( x => vec2libsvm(_label,x) )
  // vector by DenseData convert to Array[String] by libsvm format
  def data2libsvm( _arr : Iterator[Array[Double]] ) : Iterator[String] = _arr.map(vec2libsvm)
  // convert data strings to LabeledPoint in libsvm, get an Array 
  def data2libsvm( _label : Int, _arr : Iterator[Array[Double]] ) : Iterator[String] = _arr.map(x => vec2libsvm(_label,x))

  def write( _file : String, _label : Int, _arr : Array[Array[Double]] ) : Unit = {
    val w = new java.io.PrintWriter(_file)
    val content = data2libsvm(_label,_arr).mkString("\n")
    w.write(content + "\n")
    w.flush
    w.close
  }
  def write( _file : String, _label : Int, _arr : Iterator[Array[Double]] ) : Unit = {
    val w = new java.io.PrintWriter(_file)
    val contentIter = data2libsvm(_label,_arr)
    while(contentIter.hasNext) w.write(contentIter.next + "\n")
    w.flush
    w.close
  }
}

