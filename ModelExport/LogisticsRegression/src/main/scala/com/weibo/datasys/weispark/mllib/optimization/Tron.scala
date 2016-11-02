package org.apache.spark.mllib.optimization
//package tw.edu.ntu.csie.liblinear

import scala.math.exp
import scala.util.control.Breaks._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import breeze.linalg.{SparseVector => BSV,
                      DenseVector => BDV,
                      Vector=>BZV}
import org.apache.spark.storage.StorageLevel


trait HaveRegularize{
  def setRegParam(regParam: Double): this.type
}

object TronUtil extends Serializable{
  def toBreeze( _v : Vector ) : BZV[Double] = {
    _v match {
      case x : org.apache.spark.mllib.linalg.DenseVector => new BDV(_v.toArray)
      case x : org.apache.spark.mllib.linalg.SparseVector => {
        val sparseVector = _v.toSparse
new BSV(sparseVector.indices, sparseVector.values, sparseVector.size)
      }
      case _ => {new BDV(Array[Double]())}
    }
  }
}

/**
 * TronFunction defines necessary methods used for different optimization problems in TRON.
 */
abstract class TronFunction{

  def functionValue(dataPoints: RDD[(Double,Vector)], w : Vector, C : Double) : Double

  def gradient(dataPoints : RDD[(Double,Vector)], w : Vector, C : Double) : BZV[Double]

  def hessianVector(dataPoints : RDD[(Double,Vector)], w : Vector, C : Double, s : BZV[Double]) : BZV[Double]
}

/**
 * TronLR implements TronFunction for L2-regularized Logistic Regression.
 */
class TronLR extends TronFunction with Serializable
{
  def toBreeze = TronUtil.toBreeze(_)
  override def functionValue(dataPoints: RDD[(Double,Vector)], w : Vector, C : Double) : Double =
  {
    val sc = dataPoints.sparkContext
    var f_obj = sc.doubleAccumulator
    val wBz = toBreeze(w)
    dataPoints.foreach(p => {
      val featureBz = toBreeze(p._2)
      val z = featureBz.dot(wBz)
      var yz = p._1 * z
      var enyz = exp(-yz)
      if(yz >= 0)
      {
        f_obj.add( C * math.log(1+enyz) )
      }
      else
      {
        f_obj.add( C * (-yz + math.log(1+exp(yz))) )
      }
    })
    f_obj.value + (0.5 * (wBz dot wBz))
  }

  override def gradient(dataPoints : RDD[(Double,Vector)], w : Vector, C : Double) : BZV[Double] =
  {
    val n = w.size
    val wBz = toBreeze(w)
    val g = dataPoints.mapPartitions(blocks => {
      val grad = new BDV(Array.fill(n)(0.0))
      for (p <- blocks)
      {
        val featureBz = toBreeze(p._2)
        val z = featureBz dot wBz

        val zy = C * (1.0 / (1.0 + exp(-p._1 * z)) - 1.0) * p._1

        grad += featureBz :* zy
      }
      Seq(grad).iterator
    }).reduce(_ + _) + wBz
    g
  }

  override def hessianVector(dataPoints : RDD[(Double,Vector)], w : Vector, C : Double, sBz : BZV[Double]) : BZV[Double] =
  {
    val n = w.size
    val wBz = toBreeze(w)
    val Hs = dataPoints.mapPartitions(blocks => {
      var blockHs = new BDV(Array.fill(n)(0.0))
      for (p <- blocks)
      {
        val featureBz = toBreeze(p._2)
        val z = featureBz.dot(wBz)
        val wa = featureBz.dot(sBz)

        val sigma = 1.0 / (1.0 + exp(-p._1 * z))
        val D = sigma * (1.0 - sigma)
        val wa2 = C * D * wa
        blockHs += featureBz :* wa2
      }
      Seq(blockHs).iterator
    }).reduce(_ + _) + sBz
    Hs
  }
}
/*
/**
 * TronL2SVM implements TronFunction for L2-regularized L2-loss SVM.
 */
class TronL2SVM extends TronFunction
{

  override def functionValue(dataPoints: RDD[(Double,Vector)], w : Vector, C : Double) : Double =
  {
    val sc = dataPoints.sparkContext
    var f_obj = sc.accumulator(0.0)
    dataPoints.foreach(p => {
      var z = 0.0
      for(feature <- p.x)
      {
        z += feature.value * w(feature.index)
      }
      val d = 1 - p.y * z
      if (d > 0)
      {
        f_obj += C * d * d;
      }
    })
    f_obj.value + (0.5 * w dot w)
  }

  override def gradient(dataPoints : RDD[(Double,Vector)], w : Vector, C : Double) : Vector =
  {
    val n = w.length
    val g = dataPoints.mapPartitions(blocks => {
      var grad = Array.fill(n)(0.0)
      for (p <- blocks) {
        var z = 0.0
        for(feature <- p.x)
        {
          z += feature.value * w(feature.index)
        }
        z = p.y * z
        if(z < 1)
        {
          z = 2 * C * p.y * (z-1)
          for(feature <- p.x)
          {
            grad(feature.index) += z * feature.value
          }
        }
      }
      Seq(new Vector(grad)).iterator
    }).reduce(_ + _) + w
    g
  }

  override def hessianVector(dataPoints : RDD[(Double,Vector)], w : Vector, C : Double, s : Vector) : Vector =
  {
    val n = w.length
    val Hs = dataPoints.mapPartitions(blocks => {
      var blockHs = Array.fill(n)(0.0)
      for(p <- blocks)
      {
        var z = 0.0
        for(feature <- p.x)
        {
          z += feature.value * w(feature.index)
        }
        if(p.y * z < 1)
        {
          var wa = 0.0
          for(feature <- p.x)
          {
            wa += feature.value * s(feature.index)
          }
          wa = 2 * C * wa
          for(feature <- p.x)
          {
            blockHs(feature.index) += wa * feature.value
          }
        }
      }
      Seq(new Vector(blockHs)).iterator
    }).reduce(_ + _) + s
    Hs
  }
}
*/
/**
 * Tron is used to solve an optimization problem by a trust region Newton method.
 *
 * @param function a class which defines necessary methods used for the optimization problem
 */

class Tron(var function : TronFunction = new TronLR(), var cValue : Double = 1.0, var eps : Double = 0.01)
extends Optimizer with HaveRegularize
{
  def setFunction(_function:TronFunction){function = _function}
  def setEps(_eps:Double){eps=_eps}
    /**
   * Set the regularization parameter. Default 0.0.
   */
  override def setRegParam(regParam: Double): this.type = {
    this.cValue = regParam
    this
  }


  private def dnrm2_(v : BZV[Double]) : Double =
  {
    return math.sqrt(v.dot(v))
  }

  private def trcg(dataPoints : RDD[(Double,Vector)], C : Double, delta : Double, w : Vector, gBz : BZV[Double]) :
  (Int, BZV[Double], BZV[Double]) =
  {
    val n = w.size
    //var (s, r, d) = (Vector.zeros(n), Vector.zeros(n), Vector.zeros(n))
    var (rTr, rnewTrnew, beta, cgtol) = (0.0, 0.0, 0.0, 0.0)
    var s = new BDV(Array.fill(n)(0.0))
    var r = -gBz
    var d = -gBz
    cgtol = 0.1 * dnrm2_(gBz)

    var cgIter = 0
    rTr = r dot r
    breakable {
    while(true)
    {
      if(dnrm2_(r) <= cgtol)
      {
        break()
      }
      cgIter += 1

      /* hessianVector */
      var Hd = function.hessianVector(dataPoints, w, C, d)
      var alpha = rTr / (d dot Hd)
      s += d :* alpha
      if(dnrm2_(s) > delta)
      {
        println("cg reaches trust region boundary")
        alpha = -alpha
        s += d :* alpha
        val std : Double = s dot d
        val sts = s dot s
        val dtd = d dot d
        val dsq = delta*delta
        val rad = math.sqrt(std * std + dtd*(dsq-sts))
        if (std >= 0)
        {
          alpha = (dsq - sts)/(std + rad)
        }
        else
        {
          alpha = (rad - std)/dtd
        }
        s += d :* alpha
        alpha = -alpha
        r += Hd :* alpha
        break()
      }
      alpha = -alpha;
      r += Hd :* alpha
      rnewTrnew = r dot r
      beta = rnewTrnew/rTr
      d = d :* beta
      d += r
      rTr = rnewTrnew
    }
    }
    (cgIter, s, r)
  }

  /**
   * Train a model by a trust region Newton method.
   *
//   * @param prob a problem which contains data and necessary information
//   * @param param user-specified parameters
   */
  override def optimize(dataPointsIn:RDD[(Double,Vector)],initialWeights: Vector) : Vector = {
    val ITERATIONS = 1000
    val (eta0, eta1, eta2) = (1e-4, 0.25, 0.75)
    val (sigma1, sigma2, sigma3) = (0.25, 0.5, 4.0)
    var (delta, snorm) = (0.0, 0.0)
    var (alpha, f, fnew, prered, actred, gs) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    var (search, iter) = (1, 1)
    var w = initialWeights
    //val dataPoints = dataPointsIn.map(p=>(if(p._1>0.5)1.0 else -1.0, p._2)).persist(StorageLevel.MEMORY_AND_DISK_SER );
    val dataPoints = dataPointsIn.map(p=>(if(p._1>0.5)1.0 else -1.0, p._2));
    var w_new:BZV[Double] = new BDV(Array.fill(w.size)(0.0))
    var wBz = TronUtil.toBreeze(w)


      /* Function Value*/
    f = function.functionValue(dataPoints, w, cValue)

    /* gradient */
    var g = function.gradient(dataPoints, w, cValue)
    delta = dnrm2_(g)
    var gnorm1 = delta
    var gnorm = gnorm1
    if(gnorm <= eps * gnorm1)
    {
      search = 0
    }

    breakable {
    while(iter <= ITERATIONS && search == 1)
    {
      var (cgIter, s, r) = trcg(dataPoints, cValue, delta, w, g)
      w_new = TronUtil.toBreeze(w) + s
      gs = g dot s
      prered = -0.5*(gs - (s dot r))
      /* Function value */
      fnew = function.functionValue(dataPoints, Vectors.fromBreeze(w_new), cValue)

      /* Compute the actual reduction. */
      actred = f - fnew

      /* On the first iteration, adjust the initial step bound. */
      snorm = dnrm2_(s)
      if (iter == 1)
      {
        delta = math.min(delta, snorm)
      }

      /* Compute prediction alpha*snorm of the step. */
      if(fnew - f - gs <= 0)
      {
        alpha = sigma3
      }
      else
      {
        alpha = math.max(sigma1, -0.5*(gs/(fnew - f - gs)))
      }

      /* Update the trust region bound according to the ratio of actual to predicted reduction. */
      if (actred < eta0*prered)
      {
        delta = math.min(math.max(alpha, sigma1)*snorm, sigma2*delta);
      }
      else if(actred < eta1*prered)
      {
        delta = math.max(sigma1*delta, math.min(alpha*snorm, sigma2*delta))
      }
      else if (actred < eta2*prered)
      {
        delta = math.max(sigma1*delta, math.min(alpha*snorm, sigma3*delta))
      }
      else
      {
        delta = math.max(delta, math.min(alpha*snorm, sigma3*delta))
      }

      println("iter %2d act %5.3e pre %5.3e delta %5.3e f %5.3e |g| %5.3e CG %3d".format(iter, actred, prered, delta, f, gnorm, cgIter))

      if (actred > eta0*prered)
      {
        iter += 1
        w = Vectors.fromBreeze(w_new)
        f = fnew
        /* gradient */
        g = function.gradient(dataPoints, w, cValue)

        gnorm = dnrm2_(g)
        if (gnorm <= eps*gnorm1)
        {
          println("EXIT: gnorm <= eps*gnorm1")
          break()
        }
      }
      if (f < -1.0e+32)
      {
        println("WARNING: f < -1.0e+32")
        break()
      }
      if (math.abs(actred) <= 0 && prered <= 0)
      {
        println("WARNING: actred and prered <= 0")
        break()
      }
      if (math.abs(actred) <= 1.0e-12*math.abs(f) && math.abs(prered) <= 1.0e-12*math.abs(f))
      {
        println("WARNING: actred and prered too small")
        break()
      }
    }
    }
    w
  }
}
