package weiSpark.ml.classification.NaiveBayes
import LibSVMUtil._
import weiNaiveBayesModel._
import Util._
import FileHelper._
import SparkHelper._
object weiNaiveBayesExample{
  def main(args : Array[String]) = {
    sample2data(abFile.toString, rstA)
    data2libsvm(rstA, exA, 0)
    sample2data(normalFile.toString, rstN)
    data2libsvm(rstN, exN, 1)
    combineFiles(exN,exA,exC)
    val (m,acc) = getNBCAcc(exC,Array(0.6,0.4))
    model2File(m,saveFile)
    val model = readModel(saveFile)
    model.validACC(loadLibSVMFile(exC))
  }
}

