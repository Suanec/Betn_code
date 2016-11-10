  /**
  import org.apache.spark.ml.feature.{LabeledPoint => mllp}
  import org.apache.spark.ml.linalg.{Vectors => mlvs}
  import com.weibo.datasys.Common._
  import com.weibo.datasys.Common.ConfSpecs._
  import com.weibo.datasys.Macros.MacroRunnable
  import scala.reflect.runtime._

  def dataCleansing(_idxs : Array[(String,Int)], _str : String ) : Array[(String,String)] = {
    val kdata_line = _str.split('\t')
    val key_data_line = _idxs.map{
      nameIdx =>
        var value = ""
        value = nameIdx._1 match {
          case name if name.contains("gender") =>
            kdata_line(nameIdx._2) match {
            case "m" => "1"
            case "f" => "0"
            case _ => "2"
          }
          case _ => kdata_line(nameIdx._2) match {
            case "\\N" => 0.toString
            case _ => kdata_line(nameIdx._2)
          }
        }
        nameIdx._1 -> value
    }
    key_data_line
  }

  def featurePairsMappor(
                          _fmc : ConfSpecs.FeatureMapType,
                          _features : Array[(String,String)]) : Array[(String,Array[Double])] = {
    val featuresValue = _features.map{
      featurePair =>
        val featureConf:(FeatureConf, Array[FeatureContent]) = _fmc.get(featurePair._1).get
        val dataDim = featureConf._2.size
        val data = Array.fill[Double](dataDim)(0)
        val featureCategory = featureConf._1._category
        val featureFormula = featureConf._1._operation
        featureCategory match {
          case "bool" => {
            val subIdx = featurePair._2.toInt
            (subIdx >= 0 && subIdx <= 1) match {
              case true => data(subIdx) = 1
              case false => data(data.size-1) = 1
            }
          }
          case "enum" => {
            val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
            val mathFormula = runtimeMirror.reflectModule(runtimeMirror.staticModule("com.weibo.datasys.Macros." + featureFormula.capitalize)).
              instance.asInstanceOf[MacroRunnable]

            val subIdx = mathFormula.run(featurePair._2).toDouble.toInt
            (subIdx >= 0 && subIdx < dataDim) match {
              case true => data(subIdx) = 1
              case false => data(dataDim - 1) = 1
            }
          }
          case "origin" => { data(0) = featurePair._2.toDouble }
          case _ => {/**featureCategory match case _ **/ }
        }
        featurePair._1 -> data
    }
    featuresValue
  }

  **/
  
object LabeledPointParser {

  /// LR 
  val pathLR = """D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\LogisticsRegression"""
  val rstPathLR = pathLR + "\\rst"
  val dataPathLR = pathLR + "\\data"
  val dataFlagConfFileLR = dataPathLR + "\\data-flag.conf"
  val dataRepeatConfFileLR = dataPathLR + "\\data-repeat.conf"
  val featureConfFileLR = dataPathLR + "\\feature.conf"
  val dataFileLR = dataPathLR + "\\data.sample"
  val output_file_pathLR = pathLR + "\\rst\\data.lbp-"

  /// Bayes 
  val pathBayes = """D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\NaiveBayes"""
  val rstPathBayes = pathBayes + "\\rst"
  val dataPathBayes = pathBayes + "\\data"
  val dataConfFileBayes = dataPathBayes + "\\data.conf"
  val featureConfFileBayes = dataPathBayes + "\\feature.conf"
  val inputFilesBayes = Array("""D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\NaiveBayes\data\abnormal_sample.txt""",
    """D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\NaiveBayes\data\normal_sample.txt""")
  val output_file_pathBayes = pathBayes + "\\rst\\data.lbp-"

  /**
  val dataConfFile = dataFlagConfFileLR
  val dataConfFile = dataRepeatConfFileLR
  val featureConfFile = featureConfFileLR
  val output_file_path = output_file_pathLR
  val inputFiles = Array(dataFileLR)
  **/
  val inputFiles = inputFilesBayes
  val dataConfFile = dataConfFileBayes
  val featureConfFile = featureConfFileBayes
  val output_file_path = output_file_pathBayes



  def lbpParser( _str : String ) : org.apache.spark.mllib.regression.LabeledPoint = {
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.linalg.Vectors
    _str.split('[').size match {
      case 2 => {
        val pattern = """\((.{0,}),\[(.{0,})\]\)""".r 
        val pattern(labelt,valuet) = _str
        val label = labelt.toDouble
        val value = valuet.split(',').map(_.toDouble)
        new LabeledPoint(label,Vectors.dense(value))
      }
      case 3 => {
        val pattern = """\((.{0,}),\((\d{0,}),\[(.{0,})\],\[(.{0,})\]\)\)""".r 
        val pattern(labelt,sizet,idxt,valuet) = _str
        val label = labelt.toDouble
        val size = sizet.toInt
        val idx = idxt.split(',').map(_.toInt)
        val value = valuet.split(',').map(_.toDouble)
        new LabeledPoint(label,Vectors.sparse(size,idx,value))
      }
      case _ => {
        println(s"source data error with ${_str}")
        new LabeledPoint(0,Vectors.dense(Array(0d)))
      }
    }
  }

  def mlLbpParser(_str : String) : org.apache.spark.ml.feature.LabeledPoint = {
    import org.apache.spark.ml.feature.LabeledPoint
    import org.apache.spark.ml.linalg.Vectors
    _str.split('[').size match {
      case 2 => {
        val pattern = """\((.{0,}),\[(.{0,})\]\)""".r 
        val pattern(labelt,valuet) = _str
        val label = labelt.toDouble
        val value = valuet.split(',').map(_.toDouble)
        new LabeledPoint(label,Vectors.dense(value))
      }
      case 3 => {
        val pattern = """\((.{0,}),\((\d{0,}),\[(.{0,})\],\[(.{0,})\]\)\)""".r 
        val pattern(labelt,sizet,idxt,valuet) = _str
        val label = labelt.toDouble
        val size = sizet.toInt
        val idx = idxt.split(',').map(_.toInt)
        val value = valuet.split(',').map(_.toDouble)
        new LabeledPoint(label,Vectors.sparse(size,idx,value))
      }
      case _ => {
        println(s"source data error with ${_str}")
        new LabeledPoint(0,Vectors.dense(Array(0d)))
      }
    }
  }

  def featureMappor(
    _fmc_b : org.apache.spark.broadcast.Broadcast[com.weibo.datasys.Common.ConfSpecs.FeatureMapType], 
    _idxs_b : org.apache.spark.broadcast.Broadcast[Array[(String, Int)]], 
    _str : String) : Array[Double] = {
    val nameValue = dataCleansing(_idxs_b.value,_str)
    val featuresValue = featurePairsMappor(_fmc_b.value, nameValue)
    val sortedFV = featuresValue.sortBy{
      x =>
        _fmc_b.value.get(x._1).get._2.maxBy(_._idx)._idx
    }.flatMap(_._2)
    sortedFV
  }

  def GenLabeledPointExample() = {

    // import org.apache.spark.ml.feature.LabeledPoint
    // import org.apache.spark.ml.linalg.Vectors

    val label_token:String = scala.io.Source.fromFile(dataConfFile).getLines.toArray.filter(_.startsWith("@")).head
    val label_type:String = label_token match {
      case x if x.contains("flag")   => "flag"
      case x if x.contains("repeat") => "repeat"
      case x if x.contains("manual") => "manual"
      case _ => "n/a"
    }

    val dcc = ParseConfFiles.loadDataConf(dataConfFile)
    val fmc = ParseConfFiles.loadFeatureConf(featureConfFile)
    val idxs = ParseConfFiles.getColsID(dcc,fmc)
    val dcc_b = spark.sparkContext.broadcast(dcc)
    val fmc_b = spark.sparkContext.broadcast(fmc)
    val idxs_b = spark.sparkContext.broadcast(idxs)

    label_type match {
      case "flag" => {
        println("I'm flag.")
        val labelIndx:Int = label_token.split(':').last.toInt
        val rstData = spark.sparkContext.textFile(inputFiles.head).map{
          x =>
          val splits = x.split('\t');
          val label = splits(labelIndx).toInt match {
            case -1 => 0
            case _ => 1
          }

          val sortedFV = featureMappor(fmc_b,idxs_b,x)
          // new LabeledPoint(splits(labelIndx).toInt,Vectors.dense(sortedFV).toSparse)
          new mllp(label, mlvs.dense(sortedFV).toSparse)
        }

        /** Save libsvm output to HDFS. */
        val ofp = (output_file_path + 
          new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
          .format(System.currentTimeMillis))
        rstData.saveAsTextFile(ofp)
        println(s"LabeledPoint format data has been saved to: ${ofp} \n")
      }
      case "repeat" =>{
        println("I'm repeat.")
        val Array(labelOne,labelZero) = label_token.split(':').tail.map(_.toInt)
        val data = spark.sparkContext.textFile(inputFiles.head).map{
          x =>
          val splits = x.split('\t');

          val sortedFV = featureMappor(fmc_b,idxs_b,x)
          (splits(labelOne).toInt, splits(labelZero).toInt, sortedFV)
        }
        val rstData = data.flatMap{
          line =>
            val rst = (0 until line._2 ).map{
              i =>
                val label = (i < line._1) match {
                  case false => 0
                  case true => 1
                }
                // new LabeledPoint(label,Vectors.dense(sortedFV).toSparse)
                new mllp(label, mlvs.dense(line._3).toSparse)
            }
            if(rst.size != line._2 ) {
              println("Expanded lines # do not correspond to expected lines #.\n" +
                "Line with error: " + line + "\n")
              // System.exit(1)
            }
            rst
        }
        val ofp = (output_file_path + 
          new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
          .format(System.currentTimeMillis))
        rstData.saveAsTextFile(ofp)
        println(s"LabeledPoint format data has been saved to: ${ofp} \n")
      }
      case "manual" =>{
        println("I'm manual.")
        if(inputFiles.size < 2) {
          println("Error: you must specify two files in 'rawData' delimited by ','. \n")
          System.exit(1)
        }

        val aData = spark.sparkContext.textFile(inputFiles(0))
        val aRstData = aData.map(line =>
          // new LabeledPoint(0, mlvs.dense(featureMappor(fmc_b,idxs_b,line)).toSparse)
          new mllp(0, mlvs.dense(featureMappor(fmc_b,idxs_b,line)).toSparse)
            )
        val nData = spark.sparkContext.textFile(inputFiles(1))
        val nRstData = nData.map(line =>
          new mllp(1, mlvs.dense(featureMappor(fmc_b,idxs_b,line)).toSparse)
          )
        val rstData = aRstData.union(nRstData)

        /** Save libsvm output to HDFS. */
        val ofp = (output_file_path + 
          new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
          .format(System.currentTimeMillis))
        rstData.saveAsTextFile(ofp)
        println(s"LabeledPoint format data has been saved to: ${ofp} \n")
      }
      case _ => {
        println("The flag type you specified is not supported right now.\n" +
          "Flag types now supported: [flag|repeat|manual] . \n")
        // System.exit(1)}
      }
    }

  }

  /**
  sc.textFile("""D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\NaiveBayes\rst\data.lbp-2016-11-09-17-37-42""").map(mlLbpParser).count
  sc.textFile("""D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\LogisticsRegression\rst\data.lbp-2016-11-09-17-30-59""").map(mlLbpParser).count
  sc.textFile("""D:\Docs\Works_And_Jobs\Sina\Betn_code\ModelExport\LogisticsRegression\rst\data.lbp-2016-11-09-17-32-32""").map(mlLbpParser).count
  */

}