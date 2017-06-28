object createTrainData {
  import org.apache.spark.mllib.linalg.Vector
  import org.apache.spark.mllib.util.MLUtils._ 
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import scala.util.Random
  import scala.math.abs
  def main(args : Array[String]) = {
    println("args : numDataCount featureSize idxCount savePath")
    val conf = new SparkConf()
      .setAppName("createTrainData")
    val sc = new SparkContext(conf)
    val rdd = randomData(sc,args(0).toInt,args(1).toInt,args(2).toInt)
    rdd.saveAsTextFile(args(3))
    println(sc.textFile(args(3)).count + "written and read!")
  }
  def randomVec( _size : Int, _idxCount : Int ) : 
    org.apache.spark.mllib.linalg.Vector = {
    val rand = new scala.util.Random 
    val indices = (0 until _idxCount).map( i => scala.math.abs(rand.nextInt % _size)).toArray.distinct.sorted
    val values = Array.fill[Double](indices.size)(1d)
    val vec = org.apache.spark.mllib.linalg.Vectors.sparse(_size,indices,values)
    vec
  }
  def randomLbp( _labels : Array[Int], _size : Int, _idxCount : Int) : LabeledPoint = {
    val label = _labels(abs(new Random().nextInt) % _labels.size)
    new LabeledPoint(label,randomVec(_size,_idxCount))
  }
  def randomValueString( _size : Int, _idxCount : Int ) : String = {
    val rand = new scala.util.Random
    val values = (0 until _idxCount).map( i => (abs(rand.nextInt % (_size-1)) + 1) -> 1d ).toArray.distinct.sortWith( (x,y) => x._1 < y._1 ).map( x => s"${x._1}:${x._2}" ).mkString(" ")
    values
  }
  def randomRstString( _labels : Array[Int], _size : Int, _idxCount : Int) : String = {
    val label = _labels(abs(new Random().nextInt) % _labels.size)
    s"${label} " + randomValueString(_size,_idxCount)
  }
  def randomData( _sc : SparkContext, _count : Int, _size : Int, _idxCount : Int) : RDD[String] = {
    _sc.parallelize((0 until _count),40).map{
      i => randomRstString(Array(0,1),_size,_idxCount)
      }
  }
  // def test() = {
  //   val r = randomData(sc,100,109991,300)
  //   val rst = r.collect
  //   val p = new java.io.PrintWriter("123")
  //   p.write(rst.mkString("\n") + "\n")
  //   p.flush
  //   p.close
  //   loadLibSVMFile(sc,"123")
  // }
}