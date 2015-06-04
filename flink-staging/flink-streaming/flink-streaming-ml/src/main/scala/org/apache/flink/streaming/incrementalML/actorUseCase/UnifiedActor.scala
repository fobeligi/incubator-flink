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

import akka.actor._
import org.apache.flink.api.common.Plan
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.client.LocalExecutor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


class UnifiedActor(
  executionEnvironment: ExecutionEnvironment,
  streamExecutionEnvironment: StreamExecutionEnvironment)
  extends Actor{

  var batchState = true // idle batch processing
  var streamingState = true // idle batch processing

  val batchActor = context.actorOf(Props(new BatchActor(executionEnvironment.createProgramPlan())),
    name = "batchActor")

  val streamingActor = context.actorOf(Props(new StreamingActor(streamExecutionEnvironment)),
    name = "streamingActor")

  def receive = {
    case message: LaunchBatch =>
      batchState = false //batch processing working
      batchActor ! message
      //send message to bachProcessing
    case message: LaunchStreaming =>
      if (streamingState) {
        streamingState = false
        streamingActor ! message
        //launch streaming
      }
    case message: Finished =>
      if (!message.environmentFlag) {
        batchState = true
      }
  }
}


class BatchActor(batchPlan: Plan) extends Actor {

  val exec = new LocalExecutor()
  exec.start()

  def receive = {
    case mes: LaunchBatch =>
      println("--------------------------BATCH 1------------------------")
      val result = exec.executePlan(batchPlan)
      println(s"----result.batchTime: ${result.getNetRuntime}-------Launch Batch processing message received! Time to run some batches..")
      sender ! Finished(true)
    case _ =>
  }
}


class StreamingActor(streamExecutionEnvironment: StreamExecutionEnvironment) extends Actor {

  def receive = {
    case mes: LaunchStreaming =>
      val result = streamExecutionEnvironment.execute()
      System.err.println(s"**********result:${result.getNetRuntime}----------Streaming launch message received!")
    case _ =>
  }

}