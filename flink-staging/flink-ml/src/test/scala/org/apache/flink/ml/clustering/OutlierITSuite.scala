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
package org.apache.flink.ml.clustering

import org.apache.commons.math3.distribution.{NormalDistribution, UniformRealDistribution}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class OutlierITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "The KMeans implementation"

  def fixture = new {
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataPointsWithLabels = env.readTextFile("/Users/fobeligi/workspace/master-thesis/" +
      "dataSets/clustering/clusteringNoiseTrainData.txt").map {
      line => {
        var featureList = List[Double]()
        val features = line.split(',')
        for (i <- 0 until features.size - 1) {
          featureList = featureList :+ features(i).trim.toDouble
        }
        LabeledVector(features(features.size - 1).trim.toDouble, DenseVector(featureList.toArray))
      }
    }

    val uniformRealDistribution = new UniformRealDistribution(-10,10)
    var centroids = List[LabeledVector]()
    
    for (i <- 0 until 4) {
      var c = List[Double]()
      for (j<- 0 until 7) {
        c = c :+ uniformRealDistribution.sample()
      }
      centroids = centroids :+ LabeledVector(i,DenseVector(c.toArray))
    }

    val normalDistribution = new NormalDistribution(0,1)
    val Q3 = normalDistribution.inverseCumulativeProbability(0.75)
    val Q1 = normalDistribution.inverseCumulativeProbability(0.25)
    val IQR = Q3- Q1
    val threshold = Q3 + (1.5*IQR)
    println(s"----------$threshold")

    val kmeans = KMeans().setInitialCentroids(env.fromCollection(centroids)).
      setNumIterations(Clustering.iterations).setThreshold(5)

    kmeans.fit(dataPointsWithLabels.map(x=>x.vector))
  }

  it should "predict points to cluster centers" in {
    val f = fixture

    val dp = f.env.readTextFile("/Users/fobeligi/workspace/master-thesis/" +
      "dataSets/clustering/clusteringNoiseTestData.txt").map {
      line => {
        var featureList = List[Double]()
        val features = line.split(',')
        for (i <- 0 until features.size - 1) {
          featureList = featureList :+ features(i).trim.toDouble
        }
        LabeledVector(features(features.size - 1).trim.toDouble, DenseVector(featureList.toArray))
      }
    }

    val vectorsWithExpectedLabels = dp.collect()
    // create a lookup table for better matching
    val expectedMap = vectorsWithExpectedLabels map (v =>
      v.vector.asInstanceOf[DenseVector] -> v.label
      ) toMap


        // calculate the vector to cluster mapping on the plain vectors
    val plainVectors = dp.map(v => v.vector)
    val predictedVectors = f.kmeans.predict(plainVectors)

    predictedVectors.map{
      x =>
        val temp = x.vector.asBreeze.toArray.toList :+ x.label
        temp
    }.writeAsText("/Users/fobeligi/workspace/master-thesis/dataSets/clustering/" +
      "clusteringOutliers-lambda3.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    var misclustered = 0.0D
    var outliers = 0.0D

    // check if all vectors were labeled correctly
    val temp = predictedVectors.collect
    temp foreach (result => {
      val expectedLabel = expectedMap.get(result.vector.asInstanceOf[DenseVector]).get
      if (result.label != expectedLabel && result.label != -1) {
        misclustered += 1.0
      }
      if (result.label==0) {
        outliers += 1.0
      }
    })

    System.err.println(s"Misclustered points: ${misclustered/temp.size}-" +
      s"--outliers:${outliers/temp.size}")
  }
}
