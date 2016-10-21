///author := "suanec_Betn"
///data := 20160918

object colsOperate {

  def testFile(_f : String) = new java.io.File(_f).isFile

  def readFile(_f : String) = scala.io.Source.fromFile(_f).getLines

  def readFile(_f : String, _codec : String) = scala.io.Source.fromFile(_f)(_codec).getLines

  def isFileExist(_f : String) = (new java.io.File(_f).isFile) || (new java.io.File(_f).isDirectory)

  def fileWriter(_f : String) = new java.io.PrintWriter(_f)

  // example : 1.txt => 1.rst or 1.rst => 1-rst.rst
  def getRstName(_f : String) = {
    val idx = _f.indices.map(i => if(_f(i) == '.') i else -1).filter(_ >= 0)
    if(_f.substring(idx.last).equals(".rst"))
      _f.substring(0,idx.last) + "-rst.rst"
    else 
      _f.substring(0,idx.last) + ".rst"
  }

  // split line by given _separator
  def splitLine( _line : String, _separator : Char ) : Array[String] = _line.split(_separator)

  // get the max cols' number
  def getMaxColNum(_f : String, _separator : Char = '\t') : Int = {
    if(!testFile(_f)) {
      System.err.println(_f + "is not a File to calculate the cols max num")
      System.exit(1)
    }
    readFile(_f).map(splitLine(_,_separator).size).max
  }

  // range to Array[Int]
  // format of range : 
  //    1
  //    1,3
  //    1:3
  //    1:3,7:9
  //    1,3,7:9
  //     -1
  def extractRange(_range : String, _f : String = "", _separator : Char = '\t') : Array[Int] = {
    val splits = _range.trim.split(',')
    val sigleIdxSize = splits.filter(_.contains(':')).size
    val range = sigleIdxSize match {
      case 0 => splits.map(_.toInt)
      case _ => {
        val sigleIdx = splits.filter(!_.contains(':')).map(_.toInt)
        val multiIdxs = splits.filter(_.contains(':')).map{
          line => 
            val Array(start,end) = line.split(':')
            var sInt = start.toInt
            var eInt = end.toInt
            if( sInt < 0 && eInt < 0 ){
              val maxColNum = getMaxColNum(_f,_separator)
              sInt += maxColNum + 1 
              eInt += maxColNum + 1
            }
            else if(sInt < 0) sInt += getMaxColNum(_f,_separator) + 1
            else if(eInt < 0) eInt += getMaxColNum(_f,_separator) + 1
            sInt to eInt
        }.flatten
        (sigleIdx.toList ::: multiIdxs.toList toArray).sorted.distinct
      }
    }
    range
  }

  def delCols2File( 
    _in : String, 
    _range : String = "1", 
    _isDel : Boolean = true,  
    _separator : Char = '\t', 
    _out : String = ""
    ) : Unit = {
    if(!testFile(_in)) {
      System.err.println(_in + " not a File!")
      // return
      exit(1)
    }
    val iter = readFile(_in)
    val range = extractRange(_range, _in, _separator).map(_-1)
    val pIter = iter.map{
      line =>
        val splits = splitLine(line,_separator)
        _isDel match {
          case true => {splits.indices.filterNot(range.filter( i => i < splits.size).contains).map(splits).toArray}
          case false => range.filter( i => i < splits.size).map( i => splits(i) )
        }
    }
    val printer = _out match {
      case "" => fileWriter(getRstName(_in))
      case _ => fileWriter(_out)
    }
    while(pIter.hasNext) printer.write(pIter.next.mkString(_separator.toString) + "\n")
    printer.flush
    printer.close
  }
  def delCols( 
    _in : String, 
    _range : String = "1", 
    _isDel : Boolean = true,  
    _separator : Char = '\t'
    ) : Iterator[Array[String]] = {
    if(!testFile(_in)) {
      System.err.println(_in + " not a File!")
      // return
      exit(1)
    }
    val iter = readFile(_in)
    val range = extractRange(_range, _in, _separator).map(_-1)
    val pIter = iter.map{
      line =>
        val splits = splitLine(line,_separator)
        _isDel match {
          case true => {splits.indices.filterNot(range.filter( i => i < splits.size).contains).map(splits).toArray}
          case false => range.filter( i => i < splits.size).map( i => splits(i) )
        }
    }
    pIter
  }
  
  // add the _value to the before _idx
  def addCols(
    _in : String, 
    _value : String,
    _idx : Int,
    _separator : Char = '\t', 
    _out : String = "") : Unit = {
    if(!testFile(_in)) {
      System.err.println(_in + " not a File!")
      // return
      exit(1)
    }
    val iter = readFile(_in)
    val pIter = iter.map{
      line =>
        val splits = line.split(_separator)
        val idx = (_idx > splits.size + 1) match {
          case true => splits.size + 1
          case false => _idx
        }
        (splits.take(idx) :+ _value) ++ splits.takeRight(splits.size - idx)
    }
    val printer = _out match {
      case "" => fileWriter(getRstName(_in))
      case _ => fileWriter(_out)
    }
    while(pIter.hasNext) printer.write(pIter.next.mkString(_separator.toString) + "\n")
    printer.flush
    printer.close
  }

}
