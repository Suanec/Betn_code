package com.weibo.datasys.etl

import com.weibo.datasys.common._
import com.weibo.datasys.pipeline.MLRunnable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source._

/**
  * Created by wulei3 on 16/10/25.
  */

object GenLibsvm extends MLRunnable{

  def run(spark:SparkSession, input:AnyRef, conf:Map[String,String]):AnyRef = {

    val table:String = conf("table")
    val raw_data:String = conf("rawData")
    val whereStmt:String = conf("whereStmt")
    val dataConf:String = conf("dataConf")
    val featureConf:String = conf("featureConf")
    val samplingRatio:String = conf("samplingRatio")
    val divideThreshold:Option[String]  = conf.get("divideThreshold")
    val rateThreshold:Option[String] = conf.get("rateThreshold")
    val sample_data:String = conf("sampleData")
    val fieldDelimiter:String = if(conf("fieldDelimiter").equals("")) "\t"
    else conf("fieldDelimiter")

    if(HdfsHelper.hdfsExists(sample_data)) {
      println("[WARN] HDFS path " + sample_data + " already exists, " +
        "delete it or specify another path!\n")
      System.exit(1)
    }
    val label_token:String = fromFile(dataConf).getLines.toArray.filter(_.startsWith("@")).head
    val inferredLabelType:String = label_token match {
      case x if x.contains("flag")   => "flag"
      case x if x.contains("repeat") => "repeat"
      case x if x.contains("manual") => "manual"
      case _ => "n/a"
    }
    val label_type:String = if(Option(conf("labelType")).get == null || conf("labelType").equals("")) inferredLabelType
    else conf("labelType")

    val dcc = ParseConfFiles.loadDataConf(dataConf)
    val paramList = ParseConfFiles.getParamList(dcc, table)
    val fmc = ParseConfFiles.loadFeatureConf(featureConf)
    val idxs = ParseConfFiles.getColsID(dcc,fmc)
    val dcc_b = spark.sparkContext.broadcast(dcc)
    val fmc_b = spark.sparkContext.broadcast(fmc)
    val idxs_b = spark.sparkContext.broadcast(idxs)

    label_type match {
      case "flag" =>
        val labelIndx:Int = label_token.split(':').last.toInt
        //val data = spark.sparkContext.textFile(raw_data).map{
        val filteredData = if(whereStmt.equals("")) HdfsHelper.loadPath(spark, raw_data).map(_.split(fieldDelimiter))
        else DataFilter.dataFilter(spark, raw_data, dataConf, whereStmt, fieldDelimiter)
        val data = filteredData.map{
          splits:Array[String] =>
            val features = DataMappor.SingleLineMappor(spark, dcc_b.value,fmc_b.value,idxs_b.value,paramList,splits)
            (splits(labelIndx).toInt, features)
        }
        val rstData:RDD[String] = data.map{
          line =>
            val labelValue:String = if(line._1 == -1) "0" else line._1.toString
            val rst:String = labelValue + " " + line._2
            rst
        }

        val sampledResultData = if(samplingRatio.contains(":")) PosNegSampling.startSampling(rstData, samplingRatio)
        else rstData
        /** Save libsvm output to HDFS. */
        sampledResultData.saveAsTextFile(sample_data)
        println("Libsvm format data has been saved to: " + sample_data + "\n")
      case "repeat" =>
        val Array(labelOne,labelZero) = label_token.split(':').tail.map(_.toInt)
        val data = HdfsHelper.loadPath(spark, raw_data).map{
        //val data = spark.sparkContext.textFile(raw_data).map{
          x =>
            val splits = x.split(fieldDelimiter);
            val features = DataMappor.SingleLineMappor(spark, dcc_b.value,fmc_b.value,idxs_b.value,paramList,splits)
            (splits(labelOne).toInt, splits(labelZero).toInt, features)
        }
        val rstData = data.flatMap{
          line =>
            val rst = (0 until line._2 ).map{
              i =>
                (i < line._1) match {
                  case false => "0 " + line._3
                  case true => "1 " + line._3
                }
            }
            if(rst.size != line._2 ) {
              println("Expanded lines # do not correspond to expected lines #.\n" +
                "Line with error: " + line + "\n")
              System.exit(1)
            }
            rst
        }
        val sampledResultData = if(divideThreshold != None && rateThreshold != None){
          PosNegSampling.startSamplingRepeat(rstData,divideThreshold.get.toInt,rateThreshold.get.toFloat)
        } else if(samplingRatio.contains(":")) PosNegSampling.startSampling(rstData, samplingRatio)
        else rstData
        sampledResultData.saveAsTextFile(sample_data)
        println("Libsvm format data has been saved to: " + sample_data + "\n")
      case "manual" =>
        val inputFiles:Array[String] = raw_data.split(",")
        if(inputFiles.size < 2) {
          println("Error: you must specify two files in 'rawData' delimited by ','. \n")
          System.exit(1)
        }

        val aData = spark.sparkContext.textFile(inputFiles(0))
        val aStrData = aData.map(line =>
          "0 " +
          DataMappor.SingleLineMappor(
            spark, dcc_b.value,fmc_b.value,idxs_b.value,paramList,line.split(fieldDelimiter)))
        val nData = spark.sparkContext.textFile(inputFiles(1))
        val nStrData = nData.map(line =>
          "1 " +
          DataMappor.SingleLineMappor(
            spark, dcc_b.value,fmc_b.value,idxs_b.value,paramList,line.split(fieldDelimiter)))
        val rstLibsvm = aStrData.union(nStrData)

        /** Save libsvm output to HDFS. */
        rstLibsvm.saveAsTextFile(sample_data)

        println("Libsvm format data has been saved to: " + sample_data + "\n")

      case _ =>
        println("The flag type you specified is not supported right now.\n" +
          "Flag types now supported: [flag|repeat|manual] . \n")
        System.exit(1)
    }

    0.asInstanceOf[AnyRef]
  }

}

