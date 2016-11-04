/// LabeledPoint 数据样例
/// (-1.0,(95,[1,4,6,10,12,15,18,21,24,27,30,33,36,39,42,45,49,59,61,71,81,87,89],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))


  val path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\ModelExport\\LogisticsRegression\\data"
  val dataConfFile = path + "\\data.conf"
  val featureMapFile = path + "\\feature.map"
  val dataFile = path + "\\data.sample"
  val outputFile = path + "\\..\\rst\\sample_repeat_data.libsvm"

  /// 读取repeat后面俩数
  val Array(labelOne,labelZero) = scala.io.Source.fromFile(
    dataConfFile).getLines.toArray.filter(
    _.startsWith("@")).head.split(':').tail.map(_.toInt)
  /// 读取全体数据，及repeat坐标对应位置下的数据
  val data = sc.textFile(dataFile).map{ 
    x =>
      val splits = x.split('\t');
      (splits(labelOne).toInt,splits(labelZero).toInt,x)
    }
  /// dataconf读取
  val dcc = RDDConfParser.loadDataConf(dataConfFile)
  /// featuremap读取，并过滤repeat对应特征
  val fmc = RDDConfParser.loadFeatureMap(featureMapFile).filterNot( x => 
    dcc.get(x._1).get._idx == labelOne ||
    dcc.get(x._1).get._idx == labelZero )
  val idxs = RDDConfParser.getColsID(dcc,fmc)
  /// 广播变量，并过滤对应配置
  val dcc_b = sc.broadcast(dcc.filterNot( x => 
    labelOne == x._2._idx || labelZero == x._2._idx ))
  val fmc_b = sc.broadcast(fmc)
  val idxs_b = sc.broadcast(idxs)
  /// 调用datamappor生成libsvm
  val strData = data.map{
    line => 
      val labelOneTimes = line._1
      val labelZeroTimes = line._2
      val features = RDDDataMappor.RDDSingleLineMappor(fmc_b.value,dcc_b.value,idxs_b.value,line._3)
      (labelOneTimes,labelZeroTimes,features)
  }
  /// 数据展开
  val rstData = strData.flatMap{
    line => 
      val rst = (0 until line._2 ).map{
        i => 
          (i < line._1) match {
            case false => "0 " + line._3.split(' ').tail.mkString(" ")
            case true => "1 " + line._3.split(' ').tail.mkString(" ")
          }
      }//.toArray
      if(rst.size != line._2 ) println(line + "size error!")
      rst
  }
  /// 写文件
  val ofp = (outputFile + 
    new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    .format(System.currentTimeMillis))
  rstData.saveAsTextFile(ofp)

