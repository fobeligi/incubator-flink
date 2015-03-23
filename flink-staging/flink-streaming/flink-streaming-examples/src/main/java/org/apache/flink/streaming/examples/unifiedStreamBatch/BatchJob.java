/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.examples.unifiedStreamBatch;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.LocalExecutor;


public class BatchJob implements Runnable {

	private Plan plan;
	private LocalExecutor executor;
	private ExecutionEnvironment batchEnv;
	private DataSet<Tuple2<Double, Integer>> dataSet;

	public BatchJob(ExecutionEnvironment execEnv) {
		this.batchEnv = execEnv;
		this.plan = execEnv.createProgramPlan();
		this.executor = new LocalExecutor(true);
		executor.setTaskManagerNumSlots(8);
		try {
			executor.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void run() {

		try {
			executor.executePlan(plan);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}finally {
			try {
				executor.stop();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}