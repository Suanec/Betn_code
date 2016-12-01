package com.weibo.datasys.etl


object randomCreateData0 {
  import scala.util.Random 
  import scala.reflect.ClassTag
  import scala.{ specialized => spec }
  case class Value[@spec V](_value : V)

  def arrayFill[T:ClassTag](count: Int, value: T) : Array[Value[T]] = Array.fill[Value[T]](count)(Value(value))
  
  // def randomArray[T:ClassTag]( 
  //   _length : Int, _rate : Double, 
  //   _seed : Long = 2026525951L)( 
  //   _defaultValue : T = 0 ) : Array[Value[T]] = {
  //     val oneValue : T = 1
  //     val arr = arrayFill(_length,_defaultValue)
  //     val rand = new Random(_seed)
  //     arr.indices.map{
  //       i =>
  //         if(rand.nextDouble < _rate) arr(i) = Value(oneValue)
  //     }
  //     arr
  // } 
  // def randomData[T]( _numSimples : Int, 
  //   _lineLength : Int, 
  //   _sparseRate : Double, 
  //   _defaultSeed : Boolean = false, 
  //   _lineSeed : Long = 291740372954L )
  //   ( _defaultValue : T = 0)(implicit t: ClassTag[T]) : Array[Array[T]] = {
  //   val dataArray = Array.fill[Array[T]](_lineLength)(null)
  //   dataArray.indices.map{
  //     i => 
  //       if(_defaultSeed) dataArray(i) = randomArray(_lineLength,_sparseRate,_lineSeed)(_defaultValue)
  //       else dataArray(i) = randomArray(_lineLength,_sparseRate,System.currentTimeMillis)(_defaultValue)
  //   }
  //   dataArray
  // }
}

object randomCreateDoubleArray {
  import scala.util.Random 
  
  def randomArray( 
    _length : Int, _rate : Double, 
    _seed : Long = 2026525951L) : Array[Double] = {
      val arr = Array.fill[Double](_length)(0d)
        val rand = new Random(_seed)
        arr.indices.map{
          i =>
            if(rand.nextDouble < _rate) arr(i) = 1
        }
        arr
  } 
  def randomData( _numSimples : Int, 
    _lineLength : Int, 
    _sparseRate : Double, 
    _defaultSeed : Boolean = false, 
    _lineSeed : Long = 291740372954L ) : Array[Array[Double]] = {
    val dataArray = new Array[Array[Double]](_numSimples)
    dataArray.indices.map{
      i => 
        if(_defaultSeed) dataArray(i) = randomArray(_lineLength,_sparseRate,_lineSeed)
        else dataArray(i) = randomArray(_lineLength,_sparseRate,System.currentTimeMillis)
    }
    dataArray
  }
}

