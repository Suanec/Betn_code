package weiSpark.ml.classification.NaiveBayes

object FileHelper{
  import java.io.{File,PrintWriter}
  import scala.io.Source

  val path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\NaiveBayesModelExport"
  val dataPath = path + "\\data"
  val originalDataPath = path + "\\data\\original"

  val abFile = originalDataPath + "\\abnormal_sample.txt"
  val normalFile = originalDataPath + "\\normal_sample.txt"

  // (ab)normal_sample.rst to extract_(ab)normal_data.rst - libsvm 
  val rstN = originalDataPath + "\\normal_sample.rst"
  val rstA = originalDataPath + "\\abnormal_sample.rst"

  val exN = dataPath + "\\extract_normal_data.rst"
  val exA = dataPath + "\\extract_abnormal_data.rst"
  val exC = dataPath + "\\extract_combined_data.rst"

  def testFile(_f : String) = new java.io.File(_f).isFile
  def readFile(_f : String) = scala.io.Source.fromFile(_f).getLines
  def fileWriter(_f : String) = new java.io.PrintWriter(_f)
  def getOutName(_f : Array[Int]) = dataPath + "\\" + _f.mkString + ".rst"

  val saveFile = path + "\\rst\\save.model" 

}// FileHelper

