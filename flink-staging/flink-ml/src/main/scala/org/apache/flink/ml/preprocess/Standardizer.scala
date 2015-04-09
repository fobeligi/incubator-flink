/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.preprocess

import java.util

import breeze.linalg._
import breeze.numerics.sqrt
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{Vector}
import org.apache.flink.ml.preprocess.StandardScaler.{Mean, Std, DataSetSize}


//TODO::Example code here. Also a parameter description.
/** Scales observations, so that all features have mean equal to zero
  * and standard deviation equal to one
  *
  * This transformer takes a a Vector of values and maps it into the
  * scaled Vector that each feature has mean zero and standard deviation equal to one.
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.common.Learner]] implementations which expect an input of
  * [[Vector]].
  *
  * @example
  * {{{
  *                  val trainingDS: DataSet[Vector] = env.fromCollection(data)
  *
  *                  val scaler = StandardScaler().setDataSetSize(data.length)
  *                    .setMean(false)

  * }}}
  *
  * =Parameters=
  *
  * - [[StandardScaler.Mean]]: Whether to center data points to zero mean or not
  * - [[StandardScaler.Std]]: Whether to scale data points to std=1 or not
  * - [[StandardScaler.DataSetSize]]: The size of the data set
  */
class StandardScaler extends Transformer[Vector, Vector] with Serializable {

  def setMean(wm: Boolean): StandardScaler = {
    parameters.add(Mean, wm)
    this
  }

  def setStd(std: Boolean): StandardScaler = {
    parameters.add(Std, std)
    this
  }

  def setDataSetSize(size: Int): StandardScaler = {
    parameters.add(DataSetSize, size)
    this
  }

  override def transform(input: DataSet[Vector], parameters: ParameterMap):
  DataSet[Vector] = {
    val resultingParameters = this.parameters ++ parameters
    val mean = resultingParameters(Mean)
    val std = resultingParameters(Std)
    val size = resultingParameters(DataSetSize)

    val featuresMean = calculateFeaturesMean(input, size)
    val featuresStd = calculateFeaturesStd(input, featuresMean.collect.head, size)

    input.map(new RichMapFunction[Vector, Vector]() {

      var broadcastMeanSet: Vector = null
      var broadcastStdSet: Vector = null

      override def open(parameters: Configuration): Unit = {
        broadcastMeanSet = getRuntimeContext().getBroadcastVariable[Vector]("broadcastedMean").get(0)
        broadcastStdSet = getRuntimeContext().getBroadcastVariable[Vector]("broadcastedStd").get(0)
      }

      override def map(vector: Vector): Vector = {
        val myVector = vector.asBreeze
        if (mean) {
          myVector :-= broadcastMeanSet.asBreeze
          if (std) {
            myVector :/= broadcastStdSet.asBreeze
          }
        }
        else if (std) {
          myVector :/= broadcastStdSet.asBreeze
        }
        return myVector.fromBreeze
      }
    }).withBroadcastSet(featuresMean, "broadcastedMean").withBroadcastSet(featuresStd, "broadcastedStd")

  }


  private def calculateFeaturesMean(input: DataSet[Vector], size: Int): DataSet[Vector] = {
    input.reduce(new ReduceFunction[Vector] {
      override def reduce(vector1: Vector, vector2: Vector): Vector = {
        return (vector1.asBreeze + vector2.asBreeze).fromBreeze
      }
    }).map(new MapFunction[Vector, Vector] {
      override def map(vector: Vector): Vector = {
        return ((vector.asBreeze) :/ size.asInstanceOf[Double]).fromBreeze
      }
    })
  }

  private def calculateFeaturesStd(input: DataSet[Vector], featuresMean: Vector, size: Int) = {
    input.map(new MapFunction[Vector, Vector] {
      override def map(vector: Vector): Vector = {
        val t = vector.asBreeze - featuresMean.asBreeze
        return (t :* t).fromBreeze
      }
    }).reduce(new ReduceFunction[Vector] {
      override def reduce(vector1: Vector, vector2: Vector): Vector = {
        return (vector1.asBreeze + vector2.asBreeze).fromBreeze
      }
    }).map(new MapFunction[Vector, Vector] {
      override def map(vector: Vector): Vector = {
        val variance = (vector.asBreeze :/ size.asInstanceOf[Double]).fromBreeze
        for (i <- 0 until variance.size) {
          variance.update(i, sqrt(variance(i)))
        }
        return variance
      }
    })
  }


  /** Scales the vector to zero mean and unit variance
    *
    * @param vector
    * @param sMean
    * @param sStd
    */
  private def scaleVector(vector: Vector, sMean: Boolean, sStd: Boolean): Vector = {

    var myVector = vector.asBreeze

    val mean = sum(myVector) / myVector.length
    //  Standard deviation of the vector
    val t = myVector :- mean
    var std = math.sqrt(sum(t :* t) / myVector.length)
    if (std == 0.0) {
      std = 1.0
    }

    if (sMean) {
      myVector :-= mean
      if (sStd) {
        myVector :/= std
      }
    }
    else if (sStd) {
      myVector :/= std
    }
    myVector.fromBreeze
  }
}

object StandardScaler {

  case object Mean extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(true)
  }

  case object Std extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(true)
  }

  case object DataSetSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

}
