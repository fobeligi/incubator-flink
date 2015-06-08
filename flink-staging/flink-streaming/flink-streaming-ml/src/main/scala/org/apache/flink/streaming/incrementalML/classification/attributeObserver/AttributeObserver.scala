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
package org.apache.flink.streaming.incrementalML.classification.attributeObserver

/** Base trait for an attribute observer, which will incrementally update the metrics for a
  * specific attribute which can be either Nominal or Numerical
  *
  */

trait AttributeObserver[IN] extends Serializable {

  /**
   * If it is called for a Nominal attribute, it calculates the entropy of splitting in that
   * attribute.
   * If it is called for a Numerical attribute, it calculates the best value for binary splitting
   * of this attribute.
   *
   * @return a (entropy,listOfSplittingValues) tuple2. The first value of the tuple is the
   *         entropy of this attribute and the second value of the tuple is the List of values
   *         for splitting in that attribute
   */
  def getSplitEvaluationMetric: (Double, List[Double])

  /** It is called for incrementally update of the metrics for a specific attribute
    *
    * @param attribute The attribute of type [[IN]]
    */
  def updateMetricsWithAttribute(attribute: IN): Unit
}
