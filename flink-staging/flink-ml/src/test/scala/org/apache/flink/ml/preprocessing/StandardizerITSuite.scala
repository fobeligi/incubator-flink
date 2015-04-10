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
package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{Vector, DenseVector}
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest._

class StandardScalerITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Standard Scaler"

  import StandardScalerData._

  it should "first center and then properly scale the given vectors" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)
    val transformer = new StandardScaler()
    val scaledVectors = transformer.setDataSetSize(data.size).transform(dataSet).collect

    scaledVectors.length should equal(centeredExpectedVectors.length)

    scaledVectors zip centeredExpectedVectors foreach {
      case (scaledVector, expectedVector) => {
        for (i <- 0 until scaledVector.size) {
          scaledVector(i) should be(expectedVector(i) +- (0.000001))
        }
      }
    }
  }

  it should "properly scale the given vectors without centering them first" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)
    val transformer = new StandardScaler().setDataSetSize(data.size).setMean(false)
    val scaledVectors = transformer.transform(dataSet).collect

    scaledVectors.length should equal(nonCenteredExpectedVectors.length)

    scaledVectors zip nonCenteredExpectedVectors foreach {
      case (scaledVector, expectedVector) => {
        for (i <- 0 until scaledVector.size) {
          scaledVector(i) should be(expectedVector(i) +- (0.000001))
        }
      }
    }
  }

  it should "properly center the given vectors without scaling them to unit variance" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)
    val transformer = new StandardScaler().setDataSetSize(data.size).setStd(false)
    val scaledVectors = transformer.transform(dataSet).collect

    scaledVectors.length should equal(nonScaledExpectedVectors.length)

    scaledVectors zip nonScaledExpectedVectors foreach {
      case (scaledVector, expectedVector) => {
        for (i <- 0 until scaledVector.size) {
          scaledVector(i) should be(expectedVector(i) +- (0.000001))
        }
      }
    }
  }
}

object StandardScalerData {

  val data: Seq[Vector] = List(DenseVector(Array(2104.00, 3.00)),
    DenseVector(Array(1600.00, 3.00)),
    DenseVector(Array(2400.00, 3.00)),
    DenseVector(Array(1416.00, 2.00)),
    DenseVector(Array(3000.00, 4.00)),
    DenseVector(Array(1985.00, 4.00)),
    DenseVector(Array(1534.00, 3.00)),
    DenseVector(Array(1427.00, 3.00)),
    DenseVector(Array(1380.00, 3.00)),
    DenseVector(Array(1494.00, 3.00)),
    DenseVector(Array(1940.00, 4.00)),
    DenseVector(Array(2000.00, 3.00)),
    DenseVector(Array(1890.00, 3.00)),
    DenseVector(Array(4478.00, 5.00)),
    DenseVector(Array(1268.00, 3.00)),
    DenseVector(Array(2300.00, 4.00)),
    DenseVector(Array(1320.00, 2.00)),
    DenseVector(Array(1236.00, 3.00)),
    DenseVector(Array(2609.00, 4.00)),
    DenseVector(Array(3031.00, 4.00)),
    DenseVector(Array(1767.00, 3.00)),
    DenseVector(Array(1888.00, 2.00)),
    DenseVector(Array(1604.00, 3.00)),
    DenseVector(Array(1962.00, 4.00)),
    DenseVector(Array(3890.00, 3.00)),
    DenseVector(Array(1100.00, 3.00)),
    DenseVector(Array(1458.00, 3.00)),
    DenseVector(Array(2526.00, 3.00)),
    DenseVector(Array(2200.00, 3.00)),
    DenseVector(Array(2637.00, 3.00)),
    DenseVector(Array(1839.00, 2.00)),
    DenseVector(Array(1000.00, 1.00)),
    DenseVector(Array(2040.00, 4.00)),
    DenseVector(Array(3137.00, 3.00)),
    DenseVector(Array(1811.00, 4.00)),
    DenseVector(Array(1437.00, 3.00)),
    DenseVector(Array(1239.00, 3.00)),
    DenseVector(Array(2132.00, 4.00)),
    DenseVector(Array(4215.00, 4.00)),
    DenseVector(Array(2162.00, 4.00)),
    DenseVector(Array(1664.00, 2.00)),
    DenseVector(Array(2238.00, 3.00)),
    DenseVector(Array(2567.00, 4.00)),
    DenseVector(Array(1200.00, 3.00)),
    DenseVector(Array(852.00, 2.00)),
    DenseVector(Array(1852.00, 4.00)),
    DenseVector(Array(1203.00, 3.00))
  )

  val centeredExpectedVectors: Seq[Vector] = List(
    DenseVector(Array(0.131415422021048, -0.226093367577688)),
    DenseVector(Array(-0.509640697590685, -0.226093367577688)),
    DenseVector(Array(0.507908698618414, -0.226093367577688)),
    DenseVector(Array(-0.743677058718778, -1.554391902096608)),
    DenseVector(Array(1.271070745775239, 1.102205166941232)),
    DenseVector(Array(-0.019945050665056, 1.102205166941232)),
    DenseVector(Array(-0.593588522777936, -0.226093367577688)),
    DenseVector(Array(-0.729685754520903, -0.226093367577688)),
    DenseVector(Array(-0.789466781548187, -0.226093367577688)),
    DenseVector(Array(-0.644465992588391, -0.226093367577688)),
    DenseVector(Array(-0.077182204201818, 1.102205166941232)),
    DenseVector(Array(-0.000865999486135, -0.226093367577688)),
    DenseVector(Array(-0.140779041464887, -0.226093367577688)),
    DenseVector(Array(3.150993255271550, 2.430503701460152)),
    DenseVector(Array(-0.931923697017461, -0.226093367577688)),
    DenseVector(Array(0.380715024092277, 1.102205166941232)),
    DenseVector(Array(-0.865782986263870, -1.554391902096608)),
    DenseVector(Array(-0.972625672865825, -0.226093367577688)),
    DenseVector(Array(0.773743478378042, 1.102205166941232)),
    DenseVector(Array(1.310500784878341, 1.102205166941232)),
    DenseVector(Array(-0.297227261132036, -0.226093367577688)),
    DenseVector(Array(-0.143322914955409, -1.554391902096608)),
    DenseVector(Array(-0.504552950609640, -0.226093367577688)),
    DenseVector(Array(-0.049199595806068, 1.102205166941232)),
    DenseVector(Array(2.403094449057862, -0.226093367577688)),
    DenseVector(Array(-1.145609070221372, -0.226093367577688)),
    DenseVector(Array(-0.690255715417800, -0.226093367577688)),
    DenseVector(Array(0.668172728521347, -0.226093367577688)),
    DenseVector(Array(0.253521349566139, -0.226093367577688)),
    DenseVector(Array(0.809357707245360, -0.226093367577688)),
    DenseVector(Array(-0.205647815473217, -1.554391902096608)),
    DenseVector(Array(-1.272802744747510, -2.882690436615528)),
    DenseVector(Array(0.050011470324320, 1.102205166941232)),
    DenseVector(Array(1.445326079876047, -0.226093367577688)),
    DenseVector(Array(-0.241262044340535, 1.102205166941232)),
    DenseVector(Array(-0.716966387068289, -0.226093367577688)),
    DenseVector(Array(-0.968809862630041, -0.226093367577688)),
    DenseVector(Array(0.167029650888366, 1.102205166941232)),
    DenseVector(Array(2.816473891267809, 1.102205166941232)),
    DenseVector(Array(0.205187753246207, 1.102205166941232)),
    DenseVector(Array(-0.428236745893957, -1.554391902096608)),
    DenseVector(Array(0.301854945886072, -0.226093367577688)),
    DenseVector(Array(0.720322135077064, 1.102205166941232)),
    DenseVector(Array(-1.018415395695235, -0.226093367577688)),
    DenseVector(Array(-1.461049383046193, -1.554391902096608)),
    DenseVector(Array(-0.189112637784819, 1.102205166941232)),
    DenseVector(Array(-1.014599585459451, -0.226093367577688))
  )

  val nonCenteredExpectedVectors: Seq[Vector] = List(
    DenseVector(Array(2.676154912029931, 3.984895603556760)),
    DenseVector(Array(2.035098792418199, 3.984895603556760)),
    DenseVector(Array(3.052648188627298, 3.984895603556760)),
    DenseVector(Array(1.801062431290106, 2.656597069037840)),
    DenseVector(Array(3.815810235784123, 5.313194138075681)),
    DenseVector(Array(2.524794439343828, 5.313194138075681)),
    DenseVector(Array(1.951150967230948, 3.984895603556760)),
    DenseVector(Array(1.815053735487981, 3.984895603556760)),
    DenseVector(Array(1.755272708460696, 3.984895603556760)),
    DenseVector(Array(1.900273497420493, 3.984895603556760)),
    DenseVector(Array(2.467557285807066, 5.313194138075681)),
    DenseVector(Array(2.543873490522749, 3.984895603556760)),
    DenseVector(Array(2.403960448543997, 3.984895603556760)),
    DenseVector(Array(5.695732745280434, 6.641492672594601)),
    DenseVector(Array(1.612815792991423, 3.984895603556760)),
    DenseVector(Array(2.925454514101161, 5.313194138075681)),
    DenseVector(Array(1.678956503745014, 2.656597069037840)),
    DenseVector(Array(1.572113817143059, 3.984895603556760)),
    DenseVector(Array(3.318482968386926, 5.313194138075681)),
    DenseVector(Array(3.855240274887225, 5.313194138075681)),
    DenseVector(Array(2.247512228876849, 3.984895603556760)),
    DenseVector(Array(2.401416575053475, 2.656597069037840)),
    DenseVector(Array(2.040186539399244, 3.984895603556760)),
    DenseVector(Array(2.495539894202816, 5.313194138075681)),
    DenseVector(Array(4.947833939066746, 3.984895603556760)),
    DenseVector(Array(1.399130419787512, 3.984895603556760)),
    DenseVector(Array(1.854483774591084, 3.984895603556760)),
    DenseVector(Array(3.212912218530231, 3.984895603556760)),
    DenseVector(Array(2.798260839575023, 3.984895603556760)),
    DenseVector(Array(3.354097197254244, 3.984895603556760)),
    DenseVector(Array(2.339091674535667, 2.656597069037840)),
    DenseVector(Array(1.271936745261374, 1.328298534518920)),
    DenseVector(Array(2.594750960333204, 5.313194138075681)),
    DenseVector(Array(3.990065569884931, 3.984895603556760)),
    DenseVector(Array(2.303477445668349, 5.313194138075681)),
    DenseVector(Array(1.827773102940595, 3.984895603556760)),
    DenseVector(Array(1.575929627378843, 3.984895603556760)),
    DenseVector(Array(2.711769140897250, 5.313194138075681)),
    DenseVector(Array(5.361213381276692, 5.313194138075681)),
    DenseVector(Array(2.749927243255091, 5.313194138075681)),
    DenseVector(Array(2.116502744114927, 2.656597069037840)),
    DenseVector(Array(2.846594435894956, 3.984895603556760)),
    DenseVector(Array(3.265061625085948, 5.313194138075681)),
    DenseVector(Array(1.526324094313649, 3.984895603556760)),
    DenseVector(Array(1.083690106962691, 2.656597069037840)),
    DenseVector(Array(2.355626852224065, 5.313194138075681)),
    DenseVector(Array(1.530139904549433, 3.984895603556760))
  )

  val nonScaledExpectedVectors: Seq[Vector] = List(
    DenseVector(Array(103.319148936170222, -0.170212765957447)),
    DenseVector(Array(-400.680851063829778, -0.170212765957447)),
    DenseVector(Array(399.319148936170222, -0.170212765957447)),
    DenseVector(Array(-584.680851063829778, -1.170212765957447)),
    DenseVector(Array(999.319148936170222, 0.829787234042553)),
    DenseVector(Array(-15.680851063829778, 0.829787234042553)),
    DenseVector(Array(-466.680851063829778, -0.170212765957447)),
    DenseVector(Array(-573.680851063829778, -0.170212765957447)),
    DenseVector(Array(-620.680851063829778, -0.170212765957447)),
    DenseVector(Array(-506.680851063829778, -0.170212765957447)),
    DenseVector(Array(-60.680851063829778, 0.829787234042553)),
    DenseVector(Array(-0.680851063829778, -0.170212765957447)),
    DenseVector(Array(-110.680851063829778, -0.170212765957447)),
    DenseVector(Array(2477.319148936170222, 1.829787234042553)),
    DenseVector(Array(-732.680851063829778, -0.170212765957447)),
    DenseVector(Array(299.319148936170222, 0.829787234042553)),
    DenseVector(Array(-680.680851063829778, -1.170212765957447)),
    DenseVector(Array(-764.680851063829778, -0.170212765957447)),
    DenseVector(Array(608.319148936170222, 0.829787234042553)),
    DenseVector(Array(1030.319148936170222, 0.829787234042553)),
    DenseVector(Array(-233.680851063829778, -0.170212765957447)),
    DenseVector(Array(-112.680851063829778, -1.170212765957447)),
    DenseVector(Array(-396.680851063829778, -0.170212765957447)),
    DenseVector(Array(-38.680851063829778, 0.829787234042553)),
    DenseVector(Array(1889.319148936170222, -0.170212765957447)),
    DenseVector(Array(-900.680851063829778, -0.170212765957447)),
    DenseVector(Array(-542.680851063829778, -0.170212765957447)),
    DenseVector(Array(525.319148936170222, -0.170212765957447)),
    DenseVector(Array(199.319148936170222, -0.170212765957447)),
    DenseVector(Array(636.319148936170222, -0.170212765957447)),
    DenseVector(Array(-161.680851063829778, -1.170212765957447)),
    DenseVector(Array(-1000.680851063829778, -2.170212765957447)),
    DenseVector(Array(39.319148936170222, 0.829787234042553)),
    DenseVector(Array(1136.319148936170222, -0.170212765957447)),
    DenseVector(Array(-189.680851063829778, 0.829787234042553)),
    DenseVector(Array(-563.680851063829778, -0.170212765957447)),
    DenseVector(Array(-761.680851063829778, -0.170212765957447)),
    DenseVector(Array(131.319148936170222, 0.829787234042553)),
    DenseVector(Array(2214.319148936170222, 0.829787234042553)),
    DenseVector(Array(161.319148936170222, 0.829787234042553)),
    DenseVector(Array(-336.680851063829778, -1.170212765957447)),
    DenseVector(Array(237.319148936170222, -0.170212765957447)),
    DenseVector(Array(566.319148936170222, 0.829787234042553)),
    DenseVector(Array(-800.680851063829778, -0.170212765957447)),
    DenseVector(Array(-1148.680851063829778, -1.170212765957447)),
    DenseVector(Array(-148.680851063829778, 0.829787234042553)),
    DenseVector(Array(-797.680851063829778, -0.170212765957447))
  )
}
