package weiSpark.ml.classification.NaiveBayes

import Util._

object LibSVMUtil {
  // one line in String type convert to libsvm, split by space 
  // features'indices start from 1
  def str2libsvm( _line : String ) : String = {
    val splits = _line.split(' ')
    val lineData = splits.zipWithIndex.filter(!_._1.equals("0")).map( x => (x._2 + 1).toString + ":" + x._1 ).mkString(" ")
    lineData
  }
  // generate LabeledPoint in libsvm format by data String 
  def str2LPlibsvm( _label : Int, _line : String ) : String = _label.toString + " " + str2libsvm(_line)
  // Array[String] by DenseData convert to Array[String] by libsvm format
  def data2libsvm( _arr : Array[String] ) : Array[String] = _arr.map(str2libsvm)
  // convert data strings to LabeledPoint in libsvm, get an Array 
  def data2LPlivsvm( _label : Int, _arr : Array[String] ) = _arr.map( x => str2LPlibsvm(_label,x) )

  def data2libsvm( _in : String, _out : String ) = {
    val iter = scala.io.Source.fromFile(_in).getLines
    val writer = new java.io.PrintWriter(_out,"utf-8")
    writer.write(str2libsvm(iter.next))
    writer.flush
    writer.close
  }
  def data2libsvm( _in : String, _out : String, _label : Int ) = {
    val iter = scala.io.Source.fromFile(_in).getLines
    val writer = new java.io.PrintWriter(_out,"utf-8")
    while(iter.hasNext) writer.write(str2LPlibsvm(_label, extractFeatures(iter.next)) + "\n")
    writer.flush
    writer.close
  }
  // sample.txt to sample.rst, remove the uid and timestamp
  def sample2data( _in : String, _out : String ) = {
    val iter = scala.io.Source.fromFile(_in).getLines
    val writer = new java.io.PrintWriter(_out,"utf-8")
    while(iter.hasNext) writer.write(iter.next.split('\t').tail.tail.mkString(" ") + "\n")
    writer.flush
    writer.close
  }
  // extract the features
  def extractFeatures( _str : String ) : String = {
    val arrt = new Array[Array[Int]](17).map( x => new Array[Int](21) )
    val splits = _str.split(' ').map( x => if(x.toInt >= 20) 20 else x.toInt )
    splits.indices.map{
      i =>
        arrt(i)(splits(i)) = 1
    }
    arrt.flatten.mkString(" ")
  }
  // combine the normal data and the abnormal data 
  def combineFiles( _normal : String, _abnormal : String, _out : String ) = {
    val iterNormal = scala.io.Source.fromFile(_normal).getLines
    val iterAbnormal = scala.io.Source.fromFile(_abnormal).getLines
    val w = new java.io.PrintWriter(_out)
    while(iterNormal.hasNext && iterAbnormal.hasNext) {
      w.write(iterNormal.next + "\n")
      w.write(iterAbnormal.next + "\n")
    }
    while(iterNormal.hasNext) w.write(iterNormal.next + "\n")
    while(iterAbnormal.hasNext) w.write(iterAbnormal.next + "\n")
    w.flush
    w.close
  }

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
  def parseLibSVMFile( path: String ): Array[(Double, Array[Int], Array[Double])] = {
    scala.io.Source.fromFile(path).getLines
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLibSVMRecord)
      .toArray
  }
  def loadLibSVMFile( path: String, numFeatures: Int = -1): Array[Util.WeiLabeledPoint] = {
    val parsed = parseLibSVMFile(path)

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      computeNumFeatures(parsed)
    }

    parsed.map { case (label, indices, values) =>
      new Util.WeiLabeledPoint(label.toDouble,org.apache.spark.mllib.linalg.Vectors.sparse(d, indices, values))
    }
  }
}

