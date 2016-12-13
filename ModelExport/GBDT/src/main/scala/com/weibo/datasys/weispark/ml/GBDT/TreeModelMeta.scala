

class TreeModelMeta(var trees: Array[org.apache.spark.mllib.tree.model.DecisionTreeModel], var treeWeights: Array[Double]) extends Serializable {
  var version:String = "1.0";
  var loadedClassName:String = "";
  var storageType:String = "JSON"; //JSON, XML

  var algo:String = "Classification"; //Classification, Regression
  var treeAlgo:String = "Regression";
  var combiningStrategy:String = "Sum"; //Vote, Average, Sum

  var nClasses:Int = 2
  var nFeatures:Int = 0
  var nTrees:Int = 0
  var nLeaves:Int = 0
  var nNodes:Int = 0

  var impurity:String = "Gini";
  var maxDepth:Int = 10;
  var maxBins:Int = 10;

  var treesXmlString:String = "";

  //case class Metadata(
  //   algo: String,
  //  treeAlgo: String,
  //  combiningStrategy: String,
  //  treeWeights: Array[Double])

  override def toString():String={
      val sb=new StringBuilder()
      sb.append("##class=" + this.loadedClassName + "\n")
      sb.append("##version=" + this.version + "\n")
      sb.append("\n")
      sb.append("##algo=" + this.algo + "\n")
      sb.append("##treeAlgo=" + this.treeAlgo + "\n")
      sb.append("##combiningStrategy=" + this.combiningStrategy + "\n")
      sb.append("##treeWeights=" + this.treeWeights.mkString(",") + "\n")
      sb.append("\n")
      sb.append("##nClasses=" + this.nClasses + "\n")
      sb.append("##nFeatures=" + this.nFeatures + "\n")
      sb.append("##nTrees=" + this.nTrees + "\n")
      sb.append("##nLeaves=" + this.nLeaves + "\n")
      sb.append("##nNodes=" + this.nNodes + "\n")
      sb.toString()
      
  }
  def toJsonString():String={
      val sb=new StringBuilder()
      sb.append("algo "+this.algo+"\n")
      sb.append("nClasses "+this.nClasses+"\n")
      sb.append("\n")
      sb.append("nFeatures "+this.nFeatures+"\n")
      sb.append("w\n")
      sb.toString()
  }
  def toMetaString():String={
      val sb=new StringBuilder()
      sb.append("##MART\n")
      sb.append("\n")
      sb.append("##class=" + this.loadedClassName + "\n")
      sb.append("##version=" + this.version + "\n")
      sb.append("\n")
      sb.append("##algo=" + this.algo + "\n")
      sb.append("##treeAlgo=" + this.treeAlgo + "\n")
      sb.append("##combiningStrategy=" + this.combiningStrategy + "\n")
      sb.append("##treeWeights=" + this.treeWeights.mkString(",")  + "\n")
      sb.append("\n")
      sb.append("##nClasses=" + this.nClasses + "\n")
      sb.append("##nFeatures=" + this.nFeatures + "\n")
      sb.append("##nTrees=" + this.nTrees + "\n")
      sb.append("##nLeaves=" + this.nLeaves + "\n")
      sb.append("##nNodes=" + this.nNodes + "\n")
      sb.append("\n")
      sb.toString()
  }
  def toTreesString():String={
      val sb=new StringBuilder()
      sb.append("\n")
      sb.toString()
  }

  def updateMetaString(lines:String):Unit = {
      lines.split("\n").foreach(content => {
          System.out.println("content: " + content)
          val keyValue: Array[String] = content.split("=")
          if (keyValue.length == 2) {
              if (keyValue.apply(0).trim.indexOf("##class") == 0) {
                  loadedClassName = keyValue.apply(1).trim
              } else if (keyValue.apply(0).trim.indexOf("##version") == 0) {
                  version = keyValue.apply(1).trim
              } else if (keyValue.apply(0).trim.indexOf("##algo") == 0) {
                  algo = keyValue.apply(1).trim
              } else if (keyValue.apply(0).trim.indexOf("##treeAlgo") == 0) {
                  treeAlgo = keyValue.apply(1).trim
              } else if (keyValue.apply(0).trim.indexOf("##combiningStrategy") == 0) {
                  combiningStrategy = keyValue.apply(1).trim
              } else if (keyValue.apply(0).trim.indexOf("##treeWeights") == 0) {
                  treeWeights = keyValue.apply(1).trim.split(",").map(_.toDouble)
              } else if (keyValue.apply(0).trim.indexOf("##nClasses") == 0) {
                  nClasses = keyValue.apply(1).trim.toInt
              } else if (keyValue.apply(0).trim.indexOf("##nFeatures") == 0) {
                  nFeatures = keyValue.apply(1).trim.toInt
              } else if (keyValue.apply(0).trim.indexOf("##nTrees") == 0) {
                  nTrees = keyValue.apply(1).trim.toInt
              } else if (keyValue.apply(0).trim.indexOf("##nLeaves") == 0) {
                  nLeaves = keyValue.apply(1).trim.toInt
              } else if (keyValue.apply(0).trim.indexOf("##nNodes") == 0) {
                  nNodes = keyValue.apply(1).trim.toInt
              }
          }
      })
  }
}

object TreeModelMeta {

  def fromMetaString(content:String, meta:TreeModelMeta):Unit = {
      val keyValue:Array[String] = content.split("=")
      if (keyValue.length == 2) {
          if (keyValue.apply(0).trim.indexOf("##class") == 0) {
              meta.loadedClassName = keyValue.apply(1).trim
          } else if (keyValue.apply(0).trim.indexOf("##version") == 0) {
              meta.version = keyValue.apply(1).trim
          } else if (keyValue.apply(0).trim.indexOf("##algo") == 0) {
              meta.algo = keyValue.apply(1).trim
          } else if (keyValue.apply(0).trim.indexOf("##treeAlgo") == 0) {
              meta.treeAlgo = keyValue.apply(1).trim
          } else if (keyValue.apply(0).trim.indexOf("##combiningStrategy") == 0) {
              meta.combiningStrategy = keyValue.apply(1).trim
          } else if (keyValue.apply(0).trim.indexOf("##treeWeights") == 0) {
              meta.treeWeights = keyValue.apply(1).trim.asInstanceOf[Array[Double]]
          } else if (keyValue.apply(0).trim.indexOf("##nClasses") == 0) {
              meta.nClasses = keyValue.apply(1).trim.toInt
          } else if (keyValue.apply(0).trim.indexOf("##nFeatures") == 0) {
              meta.nFeatures = keyValue.apply(1).trim.toInt
          } else if (keyValue.apply(0).trim.indexOf("##nTrees") == 0) {
              meta.nTrees = keyValue.apply(1).trim.toInt
          } else if (keyValue.apply(0).trim.indexOf("##nLeaves") == 0) {
              meta.nLeaves = keyValue.apply(1).trim.toInt
          }
      }
  }

  def getTreeAlgoFromMetaString(lines:String):String = {
      var tmpTreeAlgo = "Regression"
      lines.split("\n").foreach(content => {
          val keyValue: Array[String] = content.split("=")
          if (keyValue.length == 2) {
              if (keyValue.apply(0).trim.indexOf("##treeAlgo") == 0) {
                  tmpTreeAlgo = keyValue.apply(1).trim
              }
          }
      })
      tmpTreeAlgo
  }

}
