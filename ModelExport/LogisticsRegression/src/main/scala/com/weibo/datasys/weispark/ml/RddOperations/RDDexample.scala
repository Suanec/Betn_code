package com.weibo.datasys.weispark.ml
/// @author : suanec_Betn
/// @version : 0.2
/// @data : 2016/10/24

object example {
  val path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\ModelExport\\LogisticsRegression\\data"
  val dataConfFile = path + "\\data.conf"
  val featureMapFile = path + "\\feature.map"
  val dataFile = path + "\\data.sample"
  val output_file_path = path + "\\..\\rst\\data.libsvm-"
  val file = path + "\\sample_data"
  val outputFile = path + "\\..\\rst\\sample_data.libsvm"

  def RDDTest() = {
    val sparkConf = new org.apache.spark.SparkConf()
      .setAppName("FeatureMapporExample")
      .setMaster("local")
    val sc = new org.apache.spark.SparkContext(sparkConf)
    val data = sc.textFile(dataFile)
    val dcc = RDDConfParser.loadDataConf(dataConfFile)
    val fmc = RDDConfParser.loadFeatureMap(featureMapFile)
    val idxs = RDDConfParser.getColsID(dcc,fmc)
    val dcc_b = sc.broadcast(dcc)
    val fmc_b = sc.broadcast(fmc)
    val idxs_b = sc.broadcast(idxs)
    // val strData = data.map(line => DataMappor.RDDSingleLineMappor(dcc_b.value,fmc_b.value,idxs_b.value,line))
    val strData = data.map(line => RDDDataMappor.RDDSingleLineMappor(fmc_b.value,dcc_b.value,idxs_b.value,line))
    // val strData = data.map(line => RDDSingleLineMappor(fmc,dcc,idxs,line))
    val ofp = (output_file_path + 
      new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      .format(System.currentTimeMillis))
    strData.saveAsTextFile(ofp)
    val readData = sc.textFile(ofp)
  }

  def main(args : Array[String]) : Unit = {
    RDDTest()
  }
}
