package com.weibo.datasys.weispark.ml.LogisticsRegression.RDDOperations
/// @author : suanec_Betn
/// @version : 0.2
/// @data : 2016/10/27

object FeatureEvaluation {
  val path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\ModelExport\\LogisticsRegression\\data"
  val featureFile = path + "\\feature.conf"
  val outputFile = path + "\\..\\rst\\featureEvaluation.csv"
  val modelFile = path + "\\logisticRegression.model"
  val tableHeader = "featureName,featureSubIdx,weight\n"

  def ArrayTestOrigin() = {
    val fmc = RDDConfParser.loadFeatureMap(featureFile)
    val mmc = scala.io.Source.fromFile(modelFile).getLines
    while( !mmc.next.equals("w ")){}
    val ws = mmc.toArray
    val rstArr = fmc.flatMap{
      feature => 
        val featureName = feature._1
        feature._2._2.map{
          subFeature =>
            val subIdx = subFeature._info
            val featureW = ws(subFeature._idx.toInt -1)
            featureName + "," + subIdx + "," + featureW
        }
    }.toArray.sortWith((x,y) => x.split(',').last.toDouble > y.split(',').last.toDouble)

    val p = new java.io.PrintWriter(outputFile,"utf-8")
    p.write(tableHeader)
    p.write(rstArr.mkString("\n") + "\n")
    p.flush
    p.close 
  }
  def ArrayTestAbs() = {
    val fmc = RDDConfParser.loadFeatureMap(featureFile)
    val mmc = scala.io.Source.fromFile(modelFile).getLines
    while( !mmc.next.equals("w ")){}
    val ws = mmc.toArray
    val rstArr = fmc.flatMap{
      feature => 
        val featureName = feature._1
        feature._2._2.map{
          subFeature =>
            val subIdx = subFeature._info
            val featureW = ws(subFeature._idx.toInt -1)
            featureName + "," + subIdx + "," + featureW
        }
    }.toArray.sortWith(
      (x,y) =>
        scala.math.abs(x.split(',').last.toDouble) > 
        scala.math.abs(y.split(',').last.toDouble) )

    val p = new java.io.PrintWriter(outputFile,"utf-8")
    p.write(tableHeader)
    p.write(rstArr.mkString("\n") + "\n")
    p.flush
    p.close 
  }
  def FeatureEvaluation(
    _featureFile : String, 
    _modelFile : String,
    _outputFile : String,
    _tableHeader : String,
    _isAbs : Boolean) : Unit = {
    val fmc = RDDConfParser.loadFeatureMap(_featureFile)
    val mmc = scala.io.Source.fromFile(_modelFile).getLines
    while( !mmc.next.equals("w ")){}
    val ws = mmc.toArray
    val rstArr = fmc.flatMap{
      feature => 
        val featureName = feature._1
        feature._2._2.map{
          subFeature =>
            val subIdx = subFeature._info
            val featureW = ws(subFeature._idx.toInt -1)
            featureName + "," + subIdx + "," + featureW
        }
    }.toArray.sortWith(
      (x,y) =>
        _isAbs match {
          case true => {
            scala.math.abs(x.split(',').last.toDouble) > 
            scala.math.abs(y.split(',').last.toDouble)
          }
          case false => {
            x.split(',').last.toDouble > y.split(',').last.toDouble 
          }
        })

    val p = new java.io.PrintWriter(_outputFile,"utf-8")
    p.write(_tableHeader)
    p.write(rstArr.mkString("\n") + "\n")
    p.flush
    p.close 
  }
  // def IteratorTest() = {
  //   val data = colsOperate.readFile(file,"utf-8")
  //   val dcc = ConfParser.loadDataConf(dataConfFile)
  //   val fmc = ConfParser.loadFeatureMap(featureMapFile)
  //   val idxs = ConfParser.getColsID(dcc,fmc)
  //   val colSize = ConfParser.readMaxColNum(fmc)
  //   val strData = DataMappor.featureMapToData(fmc,dcc,data)
  //   val rst = strData.map(DataMappor.dataToLibsvm)
  //   val p = new java.io.PrintWriter(outputFile + ".iter","utf-8")
  //   while(rst.hasNext) p.write(rst.next + "\n")
  //   p.flush
  //   p.close   
  // }

  // def RDDTest() = {
  //   val sparkConf = new org.apache.spark.SparkConf().setAppName("FeatureMapporExample")
  //   val sc = import org.apache.spark.SparkContext(sparkConf)
  //   val data = sc.textFile(dataFile)
  //   val dcc = RDDConfParser.loadDataConf(dataConfFile)
  //   val fmc = RDDConfParser.loadFeatureMap(featureMapFile)
  //   val idxs = RDDConfParser.getColsID(dcc,fmc)
  //   val dcc_b = sc.broadcast(dcc)
  //   val fmc_b = sc.broadcast(fmc)
  //   val idxs_b = sc.broadcast(idxs)
  //   // val strData = data.map(line => DataMappor.RDDSingleLineMappor(dcc_b.value,fmc_b.value,idxs_b.value,line))
  //   val strData = data.map(line => RDDDataMappor.RDDSingleLineMappor(fmc_b.value,dcc_b.value,idxs_b.value,line))
  //   // val strData = data.map(line => RDDSingleLineMappor(fmc,dcc,idxs,line))
  //   strData.saveAsText(outputFile)
  // }

  def main(args : Array[String]) : Unit = {
    FeatureEvaluation(featureFile,modelFile,outputFile,tableHeader,true)
  }
}
