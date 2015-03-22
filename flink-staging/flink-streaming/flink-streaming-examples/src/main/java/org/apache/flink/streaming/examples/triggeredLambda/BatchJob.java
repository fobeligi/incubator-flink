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
package org.apache.flink.streaming.examples.triggeredLambda;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.core.fs.FileSystem;


public class BatchJob implements Runnable {

	private Plan plan;
	private LocalExecutor executor;
	private ExecutionEnvironment batchEnv;
	private DataSet<Integer> dataSet;

	public BatchJob(String JAR, LocalExecutor exec) {
		this.executor = exec;
		this.batchEnv = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1",
				6123, 1, JAR);

		this.dataSet = batchEnv.fromElements(1000, 2000, 3000, 4000);
	}

	@Override
	public void run() {
		dataSet.write(new TypeSerializerOutputFormat<Integer>(), "/home/fobeligi/FlinkTmp/temp2",
				FileSystem.WriteMode.OVERWRITE);

		this.plan = batchEnv.createProgramPlan();

		while (true) {
			try {
				executor.executePlan(plan);
				Thread.sleep(5000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public DataSet<Integer> getDataSet() {
		return dataSet;
	}
}