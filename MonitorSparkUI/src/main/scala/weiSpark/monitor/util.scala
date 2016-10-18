///author := "suanec_Betn"
///data := 20160918

object Util{
  /// show the given Array
  def showArray( _arr : Array[_ <: Any] ) = _arr foreach println
  def showArrWithLineNum( _arr : Array[_ <: Any] ) = {
    var count = 0
    val arr = _arr.map{
      k =>
        count += 1
        ((count - 1),k)
    }
    showArray(arr)
  }
  /// grammar sugar 
  def showArr( _arr : Array[_ <: Any] ) = showArrWithLineNum(_arr)
}  