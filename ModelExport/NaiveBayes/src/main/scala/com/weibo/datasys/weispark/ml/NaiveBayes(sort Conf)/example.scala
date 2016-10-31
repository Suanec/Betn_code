// package weiSpark.ml.classification.NaiveBayes
package com.weibo.datasys.weispark.ml.NaiveBayes

object example extends Serializable {
  val path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\ModelExport\\NaiveBayes\\data"
  val nFile = path + "\\normal_sample.txt"
  val aFile = path + "\\abnormal_sample.txt"
  val dataConfFile = path + "\\data.conf"
  val featureConfFile = path + "\\feature.conf"
  val outputFile = path + "\\..\\rst\\NaiveBayes.libsvm.data-"

  def testNaiveBayesDataMappor() = {
    val dcc = RDDConfParser.loadDataConf(dataConfFile)
    val fmc = RDDConfParser.loadFeatureMap(featureConfFile)
    val idxs = dcc.map(_._2._idx).toArray.sorted
    val operators = dcc.toArray.sortBy(_._2._idx).map(_._2._operation)
    val maxSize = fmc.toArray.sortBy(x => dcc.get(x._1).get._idx).map(_._2._2.size)
    val dcc_b = sc.broadcast(dcc)
    val fmc_b = sc.broadcast(fmc)
    val idxs_b = sc.broadcast(idxs)
    val operators_b = sc.broadcast(operators)
    val maxSize_b = sc.broadcast(maxSize)
    val aData = sc.textFile(aFile).map(
      x => 
      DataMappor.dataMappor(
        DataMappor.delCols(x, idxs_b.value),
        idxs_b.value, 
        maxSize_b.value, 
        operators_b.value) )
    val nData = sc.textFile(nFile).map(
      x => 
      DataMappor.dataMappor(
        DataMappor.delCols(x, idxs_b.value),
        idxs_b.value, 
        maxSize_b.value, 
        operators_b.value) )
    val aRstLibsvm = aData.map(x => RDDgenerateLibSVM.vec2libsvm(1,x.map(_.toDouble)))
    val nRstLibsvm = nData.map(x => RDDgenerateLibSVM.vec2libsvm(0,x.map(_.toDouble)))
    val rstLibsvm = aRstLibsvm.union(nRstLibsvm)
    val ofp = (outputFile + 
      new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      .format(System.currentTimeMillis))
    rstLibsvm.saveAsTextFile(ofp)
    val readData = sc.textFile(ofp)
    readData.count
  }
  def main(args : Array[String]) = {
    val conf = new org.apache.spark.SparkConf()
      .setAppName("testNaiveBayesDataMappor")
      .setMaster("local")
    val sc = new org.apache.spark.SparkContext(conf)
    testNaiveBayesDataMappor()
  }
}

