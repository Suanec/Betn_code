/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.{Tron, Optimizer}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.mllib.linalg.Vectors
import scala.io.Source
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

object ModelUtil {
  def loadModelFromLiblinearTextFormat(path: String): GeneralizedLinearModel = {
    val lines = Source.fromFile(path).getLines
    var lineIndex = 0
    //read head
    try {
      lineIndex += 1
      var items = lines.next.split("\\s+")
      assert(items.size == 2)
      assert(items(0) == "solver_type")
      val LRPattern = """.*LR""".r
      items(1) match {
        case LRPattern() =>
        case _ =>
          System.err.println("Don't support this typeof Model!")
          return null
      }
      lineIndex += 1
      items = lines.next.split("\\s+")
      assert(items.size == 2)
      assert(items(0) == "nr_class")
      if (items(1).toInt != 2) {
        System.err.println("Don't support multiple classify!")
        return null
      }
      lineIndex += 1
      items = lines.next.split("\\s+")
      assert(items.size == 3)
      assert(items(0) == "label")
      //TODO!!
      assert(items(1).toDouble > 0.5)
      lineIndex += 1
      items = lines.next.split("\\s+")
      assert(items.size == 2)
      assert(items(0) == "nr_feature")
      val featureNum = items(1).toInt
      lineIndex += 1
      items = lines.next.split("\\s+")
      assert(items.size == 2)
      assert(items(0) == "bias")
      var bias = items(1).toDouble
      lineIndex += 1
      assert(lines.next == "w")
      val weights = new Array[Double](featureNum)
      weights(0) = 0.0
      for (i <- 0 until featureNum) {
        lineIndex += 1
        weights(i) = lines.next.toDouble
      }
      if (bias > 0) {
        lineIndex += 1
        bias = bias * lines.next.toDouble
      } else {
        bias = 0.0
      }
      assert(!lines.hasNext || lines.next.trim == "")
      return new LogisticRegressionModel(
        Vectors.dense(weights), bias)
    } catch {
      case _: Throwable => System.err.println(s"Format error at line $lineIndex")
    }
    return null
  }
  def saveModelToLiblinearTextFormat(model: GeneralizedLinearModel, path: String) {
    import java.io._
    val outWriter = new BufferedWriter(new FileWriter(new File(path)))

    outWriter.append(
      "solver_type L2R_LR\n" +
      "nr_class 2\n" +
      "label 1 -1\n" + //TODO!!! edit loadLibSvm in MLUtil
      s"nr_feature ${model.weights.size}\n" +
      "bias 1\n" +
      "w\n"
    )
    model.weights.toArray.foreach(value=>outWriter.append(value+"\n"))
    outWriter.append(model.intercept+"\n")
    outWriter.close()
  }
}

/**
 * Train a classification model for Logistic Regression using Stochastic Gradient Descent.
 * NOTE: Labels used in Logistic Regression should be {0, 1}
 */
class LogisticRegression(_optimizer:Optimizer=new Tron())
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {
  override val optimizer = _optimizer

  override protected val validators = List(DataValidators.binaryLabelValidator)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}

