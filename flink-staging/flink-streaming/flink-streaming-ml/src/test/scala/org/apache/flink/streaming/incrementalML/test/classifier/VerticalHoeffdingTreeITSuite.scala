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

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.classification.VerticalHoeffdingTree
import org.apache.flink.streaming.incrementalML.evaluator.PrequentialEvaluator
import org.apache.flink.streaming.incrementalML.inspector.PageHinkleyTest
import org.apache.flink.test.util.FlinkTestBase
import org.apache.flink.util.Collector
import org.scalatest.{FlatSpec, Matchers}

class VerticalHoeffdingTreeITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Vertical Hoeffding Tree algorithm"

  it should "Create the classification HT of the given data set" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val VHTParameters = ParameterMap()
    //    val nominalAttributes = Map(0 ->4, 2 ->4, 4 ->4, 6 ->4 8 ->4)

    VHTParameters.add(VerticalHoeffdingTree.MinNumberOfInstances, 60)
    VHTParameters.add(VerticalHoeffdingTree.NumberOfClasses, 3)
    VHTParameters.add(VerticalHoeffdingTree.Parallelism, 2)
    //        VHTParameters.add(VerticalHoeffdingTree.ModelParallelism, 2)
    //    VHTParameters.add(VerticalHoeffdingTree.OnlyNominalAttributes,true)
    //    VHTParameters.add(VerticalHoeffdingTree.NominalAttributes, nominalAttributes)

    val dataPoints = env.readTextFile ("/Users/fobeligi/workspace/master-thesis/dataSets/" +
      "Waveform-MOA/Waveform-2M.arrf").map {
      line => {
        var featureList = Vector[Double]()
        val features = line.split(',')
        for (i <- 0 until features.size - 1) {
          featureList = featureList :+ features(i).trim.toDouble
        }

        LabeledVector(features(features.size - 1).trim.toDouble, DenseVector(featureList.toArray))

      }
    }

    //    val transformer = Imputer()
    val vhtLearner = VerticalHoeffdingTree(env)
    val evaluator = PrequentialEvaluator()

    //    val vhtChainedLearner = new ChainedLearner[LabeledVector, LabeledVector, (Int, Metrics)](
    //      transformer, vhtLearner)

    val streamToEvaluate = vhtLearner.fit(dataPoints, VHTParameters)

    val evaluationStream = evaluator.evaluate(streamToEvaluate)

    evaluationStream.writeAsCsv("/Users/fobeligi/workspace/master-thesis/dataSets/Waveform-MOA" +
      "/sea.csv").setParallelism(1)

    val changeDetectorParameters = ParameterMap()
    changeDetectorParameters.add(PageHinkleyTest.Delta, 0.0005)
    changeDetectorParameters.add(PageHinkleyTest.Lambda, 2.2)

    val changeDetector = PageHinkleyTest()

    changeDetector.detectChange(evaluationStream.map(x => x._3).setParallelism(1),
      changeDetectorParameters).flatMap(new UnifiedStreamBatchMapper()).setParallelism(1)

    env.execute()
  }
}

class UnifiedStreamBatchMapper
  extends FlatMapFunction[Boolean, Boolean] {

  def createSubmitBatchJob(c: Int): Unit = {
    val batchEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val csvR: DataSet[(Double, Int)] = batchEnvironment.readCsvFile("/Users/fobeligi/workspace/" +
      "master-thesis/dataSets/UnifiedBatchStream.csv")

    csvR.writeAsCsv(s"/Users/fobeligi/workspace/master-thesis/dataSets/FlinkTmp/temp$c", "\n", "," +
      "", FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    //    csvR.write(new TypeSerializerOutputFormat[(Double, Int)], "/Users/fobeligi/workspace/" +
    //      "master-thesis/dataSets/FlinkTmp/temp", FileSystem.WriteMode.OVERWRITE)

    batchEnvironment.execute()
  }

  var c = 0

  override def flatMap(value: Boolean, out: Collector[Boolean]): Unit = {
    if (value) {
      System.err.println(value)
      createSubmitBatchJob(c);
      c += 1
    }
    else {
      out.collect(value)
    }
  }
}
