object example {
  val path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\ModelExport\\LogisticsRegression\\data"
  val dataConfFile = path + "\\data.conf"
  val featureMapFile = path + "\\feature.map"
  val dataFile = path + "\\data.sample"
  val file = path + "\\sample_data"
  val outputFile = path + "\\..\\rst\\sample_data.libsvm"

  def ArrayTest() = {
    val data = colsOperate.readFile(file,"utf-8").toArray
    val dcc = ConfParser.loadDataConf(dataConfFile)
    val fmc = ConfParser.loadFeatureMap(featureMapFile)
    val idxs = ConfParser.getColsID(dcc,fmc)
    val colSize = ConfParser.readMaxColNum(fmc)
    val k = data.head.split('\t')
    val kk = idxs.map(x => x._1 -> k(x._2))
    val strData = DataMappor.featureMapToData(fmc,dcc,data)
    val rst = strData.map(DataMappor.dataToLibsvm).mkString("\n")
    val p = new java.io.PrintWriter(outputFile,"utf-8")
    p.write(rst + "\n")
    p.flush
    p.close 
  }

  def IteratorTest() = {
    val data = colsOperate.readFile(file,"utf-8")
    val dcc = ConfParser.loadDataConf(dataConfFile)
    val fmc = ConfParser.loadFeatureMap(featureMapFile)
    val idxs = ConfParser.getColsID(dcc,fmc)
    val colSize = ConfParser.readMaxColNum(fmc)
    val strData = DataMappor.featureMapToData(fmc,dcc,data)
    val rst = strData.map(DataMappor.dataToLibsvm)
    val p = new java.io.PrintWriter(outputFile + ".iter","utf-8")
    while(rst.hasNext) p.write(rst.next + "\n")
    p.flush
    p.close   
  }

  def main(args : Array[String]) : Unit = {
    IteratorTest()
  }
}
