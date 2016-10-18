object example{
  def testIteratorConvert() = {
    val file = "normal_sample.txt"
    val data = colsOperate.delCols(file,"1:-15")
    val iter = generateLibSVM.data2libsvm(
      1,data
      .map( x => x.map(_.toDouble) )
      ).map(readLibSVM.parseLibSVMRecord)
    val iter1 = iter.map{
      x => 
      x._1 -> {
        val arr = new Array[Double](x._2.last+1)
        x._2.indices.map( i => arr(x._2(i)) = x._3(i) )
        arr
      }
    }
    iter1
    // println(t.next)
  }
  def main(args : Array[String]) = {
    testIteratorConvert()
  }
}