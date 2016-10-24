package com.weibo.datasys.weispark.ml
/// @author : suanec_Betn
/// @version : 0.2
/// @data : 2016/10/24


object DFgenerateLibSVM extends Serializable {

  // one vector in Array[Double] type convert to libsvm, split by space 
  // features'indices start from 1
  @transient
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
  @transient
  // generate LabeledPoint in libsvm format by data String 
  def vec2libsvm( _label : Int, _vec : Array[Double] ) : String = _label.toString + " " + vec2libsvm(_vec)

}
