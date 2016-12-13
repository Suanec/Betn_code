
import java.io._

// import com.weibo.datasys.weispark.tree.{RegressionTree, Ensemble}
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.util.Loader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import TreeModelMeta
import scala.collection.mutable
import scala.io.Source

object TreeModelUtil {
  /**
   * save the model to local file system
   */
  def saveRandomForestTreeModelToFile(sc: SparkContext, model: RandomForestModel, path: String) {
      // method 1:
      //model.save(sc, path)

      // method 2:
      import java.io._
      val outWriter = new BufferedWriter(new FileWriter(new File(path)))

      val meta = saveRandomForestModelToMeta(model)

      outWriter.append(meta.toMetaString())
      outWriter.append("\n");
      outWriter.append(meta.treesXmlString)

      outWriter.close()
  }

  def saveRandomForestModelToMeta(model: RandomForestModel):TreeModelMeta = {
      val meta = new TreeModelMeta(model.trees, Array.fill(model.trees.length)(1.0))

      meta.version = "1.0"
      meta.loadedClassName = "org.apache.spark.mllib.tree.model.RandomForestModel"

      meta.algo = model.algo.toString //"Classification"
      meta.treeAlgo = model.trees(0).algo.toString
      //meta.combiningStrategy = model.combiningStrategy.toString
      meta.combiningStrategy = if (model.algo == Algo.Classification) "Vote" else "Average"

      meta.nTrees = model.numTrees
      meta.nNodes = model.totalNumNodes

      //meta.treesXmlString = treesXmlStringFromDecisionTreeModelArray(meta.trees)
      //meta.treesXmlString = treesJsonStringFromDecisionTreeModelArray(meta.trees)
      var (treesXmlString, leavesNum) = treesJsonStringFromDecisionTreeModelArray(meta.trees, 0)
      meta.treesXmlString = treesXmlString
      meta.nLeaves = leavesNum

      meta
  }

  def saveDecisionTreeModelToFile(sc: SparkContext, model: DecisionTreeModel, path: String) {
      // method 1:
      //model.save(sc, path)

      // method 2:
      import java.io._
      val outWriter = new BufferedWriter(new FileWriter(new File(path)))

      val meta = saveDecisionTreeModelToMeta(model)

      outWriter.append(meta.toMetaString())
      outWriter.append("\n");
      outWriter.append(meta.treesXmlString)

      outWriter.close()
  }

  def saveDecisionTreeModelToMeta(model: DecisionTreeModel):TreeModelMeta = {
      val trees:Array[DecisionTreeModel] = Array.fill(1)(model)
      val meta = new TreeModelMeta(trees, Array.fill(trees.length)(1.0))

      meta.version = "1.0"
      meta.loadedClassName = "org.apache.spark.mllib.tree.DecisionTreeModel"

      meta.algo = model.algo.toString //"Classification"
      meta.nNodes = model.numNodes
      meta.maxDepth = model.depth

      //meta.treesXmlString = treesXmlStringFromDecisionTreeModelArray(meta.trees)
      //meta.treesXmlString = treesJsonStringFromDecisionTreeModelArray(meta.trees)
      var (treesXmlString, leavesNum) = treesJsonStringFromDecisionTreeModelArray(meta.trees, 0)
      meta.treesXmlString = treesXmlString
      meta.nLeaves = leavesNum

      meta
  }

  def saveGradientBoostedTreesModelToFile(sc: SparkContext, model: GradientBoostedTreesModel, path: String, firstNodeId:Int): Unit ={
      // method 1:
      //model.save(sc, path)

      // method 2:
      import java.io._
      val outWriter = new BufferedWriter(new FileWriter(new File(path)))

      val meta = saveGradientBoostedTreesModelToMeta(model, firstNodeId)

      outWriter.append(meta.toMetaString())
      outWriter.append("\n");
      outWriter.append(meta.treesXmlString)

      outWriter.close()
  }

  def saveGradientBoostedTreesModelToMeta(model: GradientBoostedTreesModel, firstNodeId:Int):TreeModelMeta = {
      val meta = new TreeModelMeta(model.trees, model.treeWeights)

      meta.version = "1.0"
      meta.loadedClassName = "org.apache.spark.mllib.tree.model.GradientBoostedTreesModel"

      meta.algo = model.algo.toString //"Classification"
      meta.treeAlgo = model.trees(0).algo.toString //"Regression"
      //meta.combiningStrategy = "Sum"
      meta.nTrees = model.numTrees
      meta.nNodes = model.totalNumNodes

      //meta.treesXmlString = treesXmlStringFromDecisionTreeModelArray(meta.trees)
      var (treesXmlString, leavesNum) = treesJsonStringFromDecisionTreeModelArray(meta.trees, firstNodeId)
      meta.treesXmlString = treesXmlString
      meta.nLeaves = leavesNum

      meta.storageType = "JSON"
      //meta.storageType = "XML"

      meta
  }

  def treesJsonStringFromDecisionTreeModelArray (treesArray:Array[DecisionTreeModel], nodeId:Int) : (String, Int) = {
      var treesJsonString: String = ""
      var newNodeId = nodeId
      var i = 0
      for (tree <- treesArray) {
          val (tmpTreesJsonString, tmpLeavesNum ) = treesJsonStringFromDecisionTreeNode(i, tree.topNode, newNodeId)
          treesJsonString += tmpTreesJsonString
          newNodeId = tmpLeavesNum // incr all
          //treesJsonString += "\n"
          i += 1
      }
      //System.out.println("nodeId: " + nodeId + ", newNodeId: " + newNodeId)
      (treesJsonString, newNodeId)
  }

  case class SplitData(
      feature: Int,
      threshold: Double,
      featureType: Int,
      categories: Seq[Double]) //{
    // def toSplit: Split = {
    //   new Split(feature, threshold, FeatureType(featureType), categories.toList)
    // }
  // }

  object SplitData {
    def apply(s: Split): SplitData = {
      SplitData(s.feature, s.threshold, s.featureType.id, s.categories)
    }

    // def apply(r: Row): SplitData = {
    //   SplitData(r.getInt(0), r.getDouble(1), r.getInt(2), r.getAs[Seq[Double]](3))
    // }
  }

  def splitToSplitData(data: Option[Split]): Option[SplitData] = data match {
    case Some(s) => Option(new SplitData(s.feature, s.threshold, s.featureType.id, s.categories))
    case None => None
  }

  def treesJsonStringFromDecisionTreeNode (treeId:Int, node:Node, nodeId:Int) : (String, Int) = {
      var treesJsonString: String = ""
      var newNodeId = nodeId
      implicit val formats = DefaultFormats

      newNodeId += 1

      var jsonStr: JsonAST.JObject = ("treeId" -> treeId) ~
        ("nodeId" -> node.id) ~
        ("predict.predict" -> node.predict.predict) ~
        ("predict.prob" -> node.predict.prob) ~
        ("predict" -> Extraction.decompose(node.predict)) ~
        ("impurity" -> node.impurity) ~
        ("isLeaf" -> node.isLeaf) ~
        ("golbalId" -> newNodeId) ~
        ("split" -> Extraction.decompose(node.split)) ~
        // ("split" -> Extraction.decompose(splitToSplitData(node.split).toArray)) ~
        ("leftNodeId" -> (if(node.leftNode.isEmpty) {-1} else {node.leftNode.get.id})) ~
        ("rightNodeId" -> (if(node.rightNode.isEmpty) {-1} else {node.rightNode.get.id})) ~
        ("infoGain" -> (if(node.stats.isEmpty) {0} else {node.stats.get.gain}))
        ("stats" -> Extraction.decompose(node.stats))
      treesJsonString += compact(render(jsonStr))
      treesJsonString += "\n"

      if (node.leftNode.isDefined && !node.leftNode.isEmpty) {
          val (tmpTreesJsonString, tmpNodeId) = treesJsonStringFromDecisionTreeNode(treeId, node.leftNode.get, newNodeId)
          treesJsonString += tmpTreesJsonString
          newNodeId = tmpNodeId
          //treesJsonString += "\n"
      }

      if (node.rightNode.isDefined && !node.rightNode.isEmpty) {
          val (tmpTreesJsonString, tmpNodeId) = treesJsonStringFromDecisionTreeNode(treeId, node.rightNode.get, newNodeId)
          treesJsonString += tmpTreesJsonString
          newNodeId = tmpNodeId
          //treesJsonString += "\n"
      }

      (treesJsonString, newNodeId)
  }
}
