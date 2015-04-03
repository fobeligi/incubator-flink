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

import breeze.linalg._
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{Vector}
import org.apache.flink.ml.preprocess.Standardizer.{ScaleMean, ScaleStd}


/** Scales observations, so that all features have mean equal to zero
  * and standard deviation equal to one
  *
  * This transformer takes a a Vector of values and maps it into the
  * scaled Vector that each feature has mean zero and standard deviation equal to one.
  */
class Standardizer extends Transformer[Vector, Vector] with Serializable {

  def setScaleMean(wm: Boolean): Standardizer = {
    parameters.add(ScaleMean, wm)
    this
  }

  def setScaleStd(std: Boolean): Standardizer = {
    parameters.add(ScaleStd, std)
    this
  }

  override def transform(input: DataSet[Vector], parameters: ParameterMap):
  DataSet[Vector] = {
    val resultingParameters = this.parameters ++ parameters
    val sMean = resultingParameters(ScaleMean)
    val sStd = resultingParameters(ScaleStd)

    input.map {
      vector => {
        scaleVector(vector, sMean, sStd)
      }
    }
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

object Standardizer {

  case object ScaleMean extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(true)
  }

  case object ScaleStd extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(true)
  }

}
