import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.util.MLUtils._ 
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
object featureCount extends Serializable {
  @transient
  def featureCount( spark : SparkSession, dataPath : String ) : Array[Array[Int]] = {
    val data = loadLibSVMFile(spark.sparkContext,dataPath)
    RDDfeatureCount(data)
  }

  @transient
  def RDDfeatureCount( data : RDD[LabeledPoint] ) : 
  Array[Array[Int]] = {
    val size = data.first.features.size
    data.aggregate(Array.ofDim[Int](2,size))(seqOpt,comOpt)
  }

  @transient
  def comOpt( arr1 : Array[Array[Int]], 
    arr2 : Array[Array[Int]]) : 
  Array[Array[Int]] = {
    arr1.indices.map{
      i => 
        arr2(i).indices.map{
          j => 
            arr1(i)(j) += arr2(i)(j)

        }
    }
    arr1
  }

  @transient
  def seqOpt( arr : Array[Array[Int]], 
    lbp : LabeledPoint ) : 
  Array[Array[Int]] = {
    val idx = lbp.features.toSparse.indices
    val label = lbp.label.toInt
    idx.map{
      i => 
        arr(label)(i) += 1
    }
    arr
  }

  @transient
  def newRst( lbp : LabeledPoint ) : Array[Array[Int]] = Array.ofDim[Int](2,lbp.features.size)

  @transient
  def clearArr( _arr : Array[Array[Int]] ) = _arr.map(l => l.indices.map(i => l(i) = 0))

  @transient
  def checkCount( spark : SparkSession, dataPath : String ) : Boolean = {
    import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}
    val aggregateRst = featureCount(spark,dataPath)
    val data = loadLibSVMFile(spark.sparkContext,dataPath)
    val neg = data.filter(_.label == 0).map(_.features.toArray).collect
    val pos = data.filter(_.label == 1).map(_.features.toArray).collect
    val negV = neg.map(x => new BDV(x.map(_.toInt))).reduce(_ + _)
    val posV = pos.map(x => new BDV(x.map(_.toInt))).reduce(_ + _)
    val aggregateV = aggregateRst.map( x => new BDV(x) )
    val negBool = (aggregateV(0) - negV).sum == 0
    val posBool = (aggregateV(1) - posV).sum == 0
    negBool && posBool
  }
}
