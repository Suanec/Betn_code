
//// val src_data = """hdfs://ns1/user/weibo_bigdata_ds/zhangtong1/hive/zhangtong1_ranking_with_user_info/dt=20161128"""38445087
//// val tar_data = """hdfs://ns1/user/weibo_bigdata_ds/wulei3/shixi_enzhao/hive/"""
//// val src_data = """hdfs://ns1/user/weibo_bigdata_ds/warehouse/mds_datastrategy_unread_pool_ctr_feature_all"""
/**
/// hadoop fs -cp hdfs://ns1/user/weibo_bigdata_ds/warehouse/mds_datastrategy_unread_pool_ctr_feature_all hdfs://ns1/user/weibo_bigdata_ds/wulei3/shixi_enzhao/warehouse/mds_datastrategy_unread_pool_ctr_feature_all
hive select count(*) from mds_datastrategy_unread_pool_ctr_feature_all = 3008716749

**/
object testData {
  val src_data_zt = """hdfs://ns1/user/weibo_bigdata_ds/zhangtong1/hive/zhangtong1_ranking_with_user_info/dt=20161128"""
  val src_data_30Y = """hdfs://ns1/user/weibo_bigdata_ds/warehouse/mds_datastrategy_unread_pool_ctr_feature_all"""
  // val src_data_local = """file:///data0/work_space/shixi_enzhao/sparkResourcesLimitProbing/pipeline.xml"""
  // val data = sc.textFile(src_data_30Y)
  val data = sc.textFile(src_data_zt)
  // val data = sc.textFile(src_data_local)
  data.first
}
object testFrame {

  object testTime {
    def testNano[T,U](f : T => U) : Long = {
      val start = System.nanoTime
      val rst = f
      println(rst)
      val end = System.nanoTime
      end - start
    }
    def testMillis[T,U](f : T => U) : Long = {
      val start = System.currentTimeMillis
      val rst = f
      println(rst)
      val end = System.currentTimeMillis
      end - start
    }
    def test[T,U](f : T => U) : Unit = {
      println("testNano : " + testNano(f))
      println("testMillis : " + testMillis(f))
    }
  }
  import testTime.test
  import com.weibo.datasys._
  import com.weibo.datasys.etl._
  import com.weibo.datasys.pipeline._
  import com.weibo.datasys.macros._
  import com.weibo.datasys.common._
  import XmlConfig._
  import scala.reflect.runtime.universe

  val xmlConfigFile = "pipeline.xml"
  val pipelineRange = "[4]"
  val configFile = xml.XML.load(xmlConfigFile)
  val sparkConf = XmlConfig.loadSparkConf(configFile)
  val pipelineConf = XmlConfig.loadPipelineConf(configFile)
  val (pipelineName, pipelineCon) = pipelineConf.head
  val tasksToBeRun:Array[Int] = pipelineRange.drop(1).dropRight(1).split(",").map(_.trim.toInt)
  val tasks = pipelineCon.tasks
    val ordered = tasks.
      filter(x => tasksToBeRun.contains(x.id)).
      sortWith((x, y) => if (x.id <= y.id) true else false)
    val processes = scala.collection.mutable.Map.empty[Int, AnyRef]
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    // for (task <- ordered) {
    val task = ordered.head
      val id = task.id
      val preid = task.preid
      val taskConf = task.taskConf
      val taskObjName:MLRunnable = runtimeMirror.reflectModule(runtimeMirror.staticModule(taskConf.objectname)).
        instance.asInstanceOf[MLRunnable]
    val conf = taskConf.parameters
    val raw_data:String = conf("rawData")
    val whereStmt:String = conf("whereStmt")
    val dataConf:String = conf("dataConf")
    val featureConf:String = conf("featureConf")
    val samplingRatio:String = conf("samplingRatio")
    val sample_data:String = conf("sampleData")
    val fieldDelimiter:String = if(conf("fieldDelimiter").equals("")) "\t" else conf("fieldDelimiter")

    val label_token:String = scala.io.Source.fromFile(dataConf).getLines.toArray.filter(_.startsWith("@")).head
    val inferredLabelType:String = label_token match {
      case x if x.contains("flag")   => "flag"
      case x if x.contains("repeat") => "repeat"
      case x if x.contains("manual") => "manual"
      case _ => "n/a"
    }
    val label_type:String = if(Option(conf("labelType")).get == null || conf("labelType").equals("")) inferredLabelType else conf("labelType")

    val dcc = ParseConfFiles.loadDataConf(dataConf)
    val fmc = ParseConfFiles.loadFeatureConf(featureConf)
    val idxs = ParseConfFiles.getColsID(dcc,fmc)
    val dcc_b = spark.sparkContext.broadcast(dcc)
    val fmc_b = spark.sparkContext.broadcast(fmc)
    val idxs_b = spark.sparkContext.broadcast(idxs)

    val labelIndx:Int = label_token.split(':').last.toInt
    // val t = sc.textFile(raw_data).take(10)
    val filteredData = sc.textFile(raw_data).first.split('\t');

    val data = Array(filteredData).map{
      splits:Array[String] =>
        val features = DataMappor.SingleLineMappor(dcc_b.value,fmc_b.value,idxs_b.value,splits)
        (splits(labelIndx).toInt, features)
    }
    val rstData:Array[String] = data.map{
      line =>
        val labelValue:String = if(line._1 == -1) "0" else line._1.toString
        val rst:String = labelValue + " " + line._2
        rst
    }


    val sDate = new Date 
    val start = System.nanoTime 
    DataMappor.SingleLineMappor(dcc_b.value,fmc_b.value,idxs_b.value,splits)
    val end = System.nanoTime
    val eDate = new Date
    println(sDate -> eDate)
    println("\n" + "DataMappor.SingleLineMappor(dcc_b.value,fmc_b.value,idxs_b.value,splits)" + "\n" + "cost Time : " + (end - start)/1000d/1e6 )


        val rstData = data.flatMap{
      line =>
        val rst:String = line._1.toString + " " + line._2
        rst
    }
        rstData.saveAsTextFile(sample_data)
    println("Libsvm format data has been saved to: " + sample_data + "\n")


//     val _dcc = dcc_b.value
//     val _fmc = fmc_b.value
//     val _idxs = idxs_b.value
//     val _splits = splits
// def dataCleansing(_idxs : Array[(String,Int)], _splits : Array[String] ) : Array[(String,String)] = {
//     val key_data_line:Array[(String,String)] = _idxs.map{
//       nameIdx =>
//         var value = ""
//         value = nameIdx._1 match {
//           case name if name.contains("gender") =>
//             _splits(nameIdx._2) match {
//             case "m" => "1"
//             case "f" => "0"
//             case _ => "2"
//           }
//           case _ => _splits(nameIdx._2) match {
//             case "\\N" => 0.toString
//             case _ => _splits(nameIdx._2)
//           }
//         }
//         (nameIdx._1, value)
//     }
//     key_data_line
//   }
//       val nameValue = dataCleansing(_idxs, _splits)
// import com.weibo.datasys.common.ConfSpecs._
//   def overlapCross(contentA : Array[Double],
//                    contentB : Array[Double]) : Array[Double] = {
//     var rstValue : List[Double] = List.empty[Double]
//     contentA.map{
//       x =>
//         x match {
//           case 0 => rstValue ++= List.fill(contentB.size)(0d)
//           case 1 => rstValue ++= contentB
//           case _ => {
//             println(s"Raw feature mapped values have issue: ${x}.")
//             System.exit(1)
//           }
//         }
//     }
//     rstValue.toArray
//   }
//   val _features  = nameValue

//   /// featurePairsMappor
//     val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
//     val hasCompoundFeatures:Boolean = _fmc.map(_._2._1._category).toArray.contains("compound")
//     val rawFeaturesValues = _features.map{
//       featurePair =>
//         val dataConf:DataConf = _dcc.get(featurePair._1).get
//         val featureConf:(FeatureConf, Array[FeatureContent]) = _fmc.get(featurePair._1).get
//         val dataDim = featureConf._2.size
//         val data = Array.fill[Double](dataDim)(0)
//         val featureCategory = featureConf._1._category
//         val featureFormula = featureConf._1._operation
//         val featureArgs    = dataConf._args
//         featureCategory match {
//           case "bool" => {
//             val subIdx = featurePair._2.toInt
//             (subIdx >= 0 && subIdx <= 1) match {
//               case true => data(subIdx) = 1
//               case false => data(data.size-1) = 1
//             }
//           }
//           case "enum" => {
//             val mathFormula = runtimeMirror.reflectModule(runtimeMirror.staticModule("com.weibo.datasys.macros." + featureFormula.capitalize)).
//               instance.asInstanceOf[MacroRunnable]

//             val subIdx = mathFormula.run(featurePair._2)(featureArgs).toDouble.toInt
//             (subIdx >= 0 && subIdx < dataDim) match {
//               case true => data(subIdx) = 1
//               case false => data(dataDim - 1) = 1
//             }
//           }
//           case "origin" => { data(0) = featurePair._2.toDouble }
//           case "compound" =>
//           case _ => {/**featureCategory match case _ **/ }
//         }
//         featurePair._1 -> data
//     }
//     val compoundFeatures:ConfSpecs.FeatureMapType = _fmc.filter(_._2._1._category.equals("compound"))
// /// spark-shell finished!!

//     // if(hasCompoundFeatures) {
//       val rawFeaturesMap = rawFeaturesValues.toMap

//      val fm = compoundFeatures.head
//           val featureName = fm._2._1._name
//           val featureElems: Array[String] = fm._2._1._operation.split("\\+")
//           val elemValues: Array[Array[Double]] = featureElems.map {
//             case featureName: String =>
//               rawFeaturesMap.get(featureName).get
//           }



//       val compoundFeaturesValues: Array[(String, Array[Double])] = compoundFeatures.map {
//         case fm =>

//           val featureValue: Array[Double] = elemValues.reduce((x, y) => overlapCross(x, y))
//           (featureName, featureValue)
//       }.toArray
//       return Array.concat(rawFeaturesValues,compoundFeaturesValues)
    // }
    // rawFeaturesValues
  }



        val features = DataMappor.SingleLineMappor(dcc_b.value,fmc_b.value,idxs_b.value,splits)
        (splits(labelIndx).toInt, features)
    // }
        val rstData = data.flatMap{
      line =>
        val rst:String = line._1.toString + " " + line._2
        rst
    }
        rstData.saveAsTextFile(sample_data)
    println("Libsvm format data has been saved to: " + sample_data + "\n")







}
