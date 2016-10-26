package com.weibo.datasys.weispark.ml.LogisticsRegression.DFOperations
/// @author : suanec_Betn
/// @version : 0.2
/// @data : 2016/10/24

object example {
  val path = new java.io.File("").getAbsolutePath
  val dataConfFile = path + "/data.conf"
  val featureMapFile = path + "/feature.map"
  val dataFile = path + "/data.sample"
  val output_file_path = path + "/../rst/data.libsvm-"
  val file = path + "/sample_data"
  val outputFile = path + "/../rst/sample_data.libsvm"

  def DFTest() = {
    // val sparkConf = new org.apache.spark.SparkConf()
    //   .setAppName("FeatureMapporExample")
    //   .setMaster("local")
    // val sc = new org.apache.spark.SparkContext(sparkConf)
    val spark_Session = org.apache.spark.sql.SparkSession
      .builder
      .appName("DFOperations.DataMappor.example")
      .getOrCreate()
    import spark_Session.implicits._
    val data = spark_Session.read.textFile(dataFile)
    val dcc = DFConfParser.loadDataConf(dataConfFile)
    val fmc = DFConfParser.loadFeatureMap(featureMapFile)
    val idxs = DFConfParser.getColsID(dcc,fmc)
    val dcc_b = spark_Session.sqlContext.sparkContext.broadcast(dcc)
    val fmc_b = spark_Session.sqlContext.sparkContext.broadcast(fmc)
    val idxs_b = spark_Session.sqlContext.sparkContext.broadcast(idxs)
    // val strData = data.map(line => DataMappor.RDDSingleLineMappor(dcc_b.value,fmc_b.value,idxs_b.value,line))
    val strData = data.map(line => DFDataMappor.SingleLineMappor(fmc_b.value,dcc_b.value,idxs_b.value,line))
    // val strData = data.map(line => RDDSingleLineMappor(fmc,dcc,idxs,line))
    val ofp = (output_file_path + 
      new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      .format(System.currentTimeMillis))
    println(s"strData.size : ${strData.count}")
    strData.write.text(ofp)
    val readData = spark_Session.read.text(ofp)
    println(s"readData.size : ${readData.count}")
  }

  def main(args : Array[String]) : Unit = {
    DFTest()
  }
}
