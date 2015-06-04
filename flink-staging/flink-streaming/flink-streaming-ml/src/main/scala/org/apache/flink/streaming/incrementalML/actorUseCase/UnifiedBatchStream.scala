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
package org.apache.flink.streaming.incrementalML.actorUseCase

import akka.actor.{ActorSystem, Props}
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction
import org.apache.flink.streaming.api.scala._


object UnifiedBatchStream {
  private val JARDependencies: String = "/Users/fobeligi/workspace/flink/flink-staging/" +
    "flink-streaming/flink-streaming-examples/target/flink-streaming-examples-0" +
    ".9-SNAPSHOT-LambdaTriggeredJoin.jar"


  def main(args: Array[String]): Unit = {

    val batchEnvironment: ExecutionEnvironment = ExecutionEnvironment.createRemoteEnvironment(
      "127.0.0.1", 6123, JARDependencies)

    val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment
      .createRemoteEnvironment("127.0.0.1", 6123, 1, JARDependencies)

    val batchDataSet: DataSet[(Double, Int)] = batchEnvironment.readCsvFile("/Users/" +
      "fobeligi/workspace/master-thesis/dataSets/UnifiedBatchStream.csv")

    System.err.println("---------------------1---------------------------")
    batchDataSet.write(new TypeSerializerOutputFormat[(Double,Int)], "/Users/fobeligi/workspace/" +
      "master-thesis/dataSets/FlinkTmp/", FileSystem.WriteMode.OVERWRITE)

    val dataSetStream: DataStream[String] = streamEnvironment.readFileStream("file:///Users/" +
      "fobeligi/workspace/master-thesis/dataSets/FlinkTmp/", 1000,
      FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED)

    System.err.println("---------------------2---------------------------")

    val ds:DataStream[(Double, Int)] = dataSetStream.map(line => {

      val temp: Array[String] = line.split(",")

      (temp(0).trim.toDouble, temp(1).trim.toInt)
    })

    ds.print
    
    //---------------------------------------------------------------------------------------
    //------------------------create actors and start them-----------------------------------
    // Create an Akka system
    val system = ActorSystem("UnifiedBatchStream")

    // create the master
    val master = system.actorOf(Props(new UnifiedActor(batchEnvironment,streamEnvironment)),
      name = "masterActor")
    master ! LaunchBatch(true)
    master ! LaunchStreaming(true)
  }

}