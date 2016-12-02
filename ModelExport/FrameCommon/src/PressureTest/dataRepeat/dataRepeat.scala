import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
// import org.apache.spark.
// import org.apache.spark.
// import org.apache.spark.
// import org.apache.spark.
object dataRepeat {
  def repeatData( spark : SparkSession,
    dataPath : String, 
    targetPath : String,
    targetNum : Long ) : Long = {
    val data = spark.sparkContext.textFile(dataPath)
    val srcSize : Long = data.count
    val multiple : Long = targetNum/srcSize
    val dataArr = Array.fill(multiple.toInt)(data)
    val multiData = dataArr.reduce(_ union _)
    multiData.saveAsTextFile(targetPath)
    spark.sparkContext.textFile(targetPath).count
  }
  def rddRepeat( _rdd : RDD[_], _targetNum : Long ) : RDD[_] = {
    val srcSize = _rdd.count
    val multiple = _targetNum / srcSize
    val dataArr = Array.fill(multiple.toInt)(_rdd)
    val multiData = dataArr.reduce(_ union _)
    multiData
  }
  def main(args : Array[String]) : Unit = {
    println("args: dataPath targetPath targetNum")
    val spark = SparkSession
      .builder
      .appName("dataRepeat")
      .getOrCreate()
    println(repeatData(spark,args(0),args(1),args(2).toLong))
  }
}