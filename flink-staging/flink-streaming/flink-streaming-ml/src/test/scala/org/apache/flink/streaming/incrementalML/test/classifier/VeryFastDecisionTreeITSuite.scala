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
package org.apache.flink.streaming.incrementalML.test.classifier

import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.classification.VeryFastDecisionTree
import org.apache.flink.streaming.incrementalML.evaluator.PrequentialEvaluator
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class VeryFastDecisionTreeITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Very Fast Decision Tree algorithm"

  it should "Create the classification HT of the given data set" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameters = ParameterMap()
    //    val nominalAttributes = Map(0 ->4, 2 ->4, 4 ->4, 6 ->4 8 ->4)

    parameters.add(VeryFastDecisionTree.MinNumberOfInstances, 300)
    parameters.add(VeryFastDecisionTree.NumberOfClasses, 4)
    parameters.add(VeryFastDecisionTree.Parallelism, 2)

//    parameters.add(VeryFastDecisionTree.OnlyNominalAttributes,true)
    //    parameters.add(VeryFastDecisionTree.NominalAttributes, nominalAttributes)

    val dataPoints = env.readTextFile("/Users/fobeligi/workspace/master-thesis/dataSets/randomRBF/randomRBF-10M.arff").map {
      line => {
        var featureList = Vector[Double]()
        val features = line.split(',')
        for (i <- 0 until features.size - 1) {
          featureList = featureList :+ features(i).trim.toDouble
        }

        LabeledVector(features(features.size - 1).trim.toDouble, DenseVector(featureList.toArray))
//          features(i).trim
//          match {
//            case "?" =>
//              featureList = featureList :+ Double.NaN
//            case _ => {
              //              if (nominalAttributes.contains(i)) {
              ////                val temp =  MurmurHash3.stringHash(features(i)).toDouble
              ////                System.err.println(features(i) + "->" +temp)
              //                featureList = featureList :+ MurmurHash3.stringHash(features(i))
              // .toDouble
              //              }
              //              else {
//              featureList = featureList :+ features(i).toDouble
              //              }
//          }
//        }
        //        val vector = if (features(features.size - 1).trim equals ">50K") {
        //          LabeledVector(1, DenseVector(featureList.toArray))
        //        }
        //        else {
        //          LabeledVector(-1, DenseVector(featureList.toArray))
        //        }
//        LabeledVector(features(0).trim.toDouble, DenseVector(featureList.toArray))
      }
    }

//        val dataPoints = StreamingMLUtils.readLibSVM(env,"/Users/fobeligi/Documents/dataSets/forestCovertype/covtype.libsvm.binary.scale", 54)

    //    val transformer = Imputer()
    val vfdtLearner = VeryFastDecisionTree(env)
    val evaluator = PrequentialEvaluator()

    //    val vfdtChainedLearner = new ChainedLearner[LabeledVector, LabeledVector, (Int, Metrics)](
    //      transformer, vfdtLearner)

    val streamToEvaluate = vfdtLearner.fit(dataPoints, parameters)

    evaluator.evaluate(streamToEvaluate).writeAsText("/Users/fobeligi/workspace/master-thesis/dataSets/randomRBF/2_randomRBF-10M-parall_3_8.txt").setParallelism(1)

    env.execute()
  }
}
