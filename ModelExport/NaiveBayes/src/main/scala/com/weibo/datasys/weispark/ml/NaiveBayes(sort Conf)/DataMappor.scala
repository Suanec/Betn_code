// package weiSpark.ml.classification.NaiveBayes
package com.weibo.datasys.weispark.ml.NaiveBayes

object DataMappor extends Serializable {

  def delCols(_str : String, _idxs : Array[Int]) : Array[String] = {
    val splits = _str.split('\t')
    val size = splits.size
    if(size <= _idxs.last-1) {
      println(s"error of input by col size is ${size}, for without label less than ${_idxs.last-1}")
      return Array("")
    }
    val rst = _idxs.map( i => splits(i-1) )
    rst
  }
  def dataMappor(_arr : Array[String], 
    _idxs : Array[Int], 
    _maxSize : Array[Int],
    _operators : Array[String]) : Array[Double] = {
    val rst = new Array[Array[Double]](_idxs.size).indices.map( i => new Array[Double](_maxSize(i))).toArray
    _arr.indices.map{
      i => 
        val subIdx = _operators(i) match {
          case "div1" => _arr(i).toDouble/1
          case _ => scala.math.log(_arr(i).toDouble)
        }
        (subIdx >= 0 && subIdx < _maxSize(i)) match {
          case true => rst(i)(subIdx.toInt) = 1
          case false => rst(i)(_maxSize(i)-1) = 1
        }
    }
    rst.flatten
  }
}

