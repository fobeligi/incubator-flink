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

package org.apache.flink.streaming.examples.lambda;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.FileMonitoringFunction.WatchType;

import java.util.Random;

public class LambdaJoin {

	private static final Logger log = LoggerFactory.getLogger(LambdaJoin.class);

	private static final String JAR = "/home/fobeligi/workspace/incubator-flink/flink-staging/flink-streaming/flink-streaming-examples/target/flink-streaming-examples-0.9-SNAPSHOT-LambdaJoin.jar";

	public static LocalExecutor exec = new LocalExecutor(false);

	private static Random randomGenerator = new Random();

	public static void main(String[] args) throws Exception {

		exec.setTaskManagerNumSlots(8);
		exec.start();

		final ExecutionEnvironment batchEnv = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1",
				6123, 1, JAR);

		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createRemoteEnvironment(
				"127.0.0.1", 6123, 1, JAR);

		DataSet<Integer> dataSet = batchEnv.fromElements(1000, 2000, 3000, 4000).map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return value + randomGenerator.nextInt(70);
			}
		});
		dataSet.write(new TypeSerializerOutputFormat<Integer>(), "/home/fobeligi/FlinkTmp/temp",
				WriteMode.OVERWRITE);


		DataStream<Tuple2<String, Integer>> dataSetStream = streamEnv.readFileStream(
				"file:///home/fobeligi/FlinkTmp", dataSet.getType(), 1000,
				WatchType.REPROCESS_WITH_APPENDED);

		dataSetStream.print();
//		dataSetStream.project(1).types(Integer.class).print();

		try {
			runPeriodically(batchEnv, 5000);
			streamEnv.execute();
		} finally {
			exec.stop();
		}

	}

	private static void runPeriodically(ExecutionEnvironment env, long millis) {
		new Thread(new PeriodicJob(env, millis)).start();
	}

	private static class PeriodicJob implements Runnable {

		private Plan plan;
		private long millis;

		public PeriodicJob(ExecutionEnvironment env, long millis) {
			this.plan = env.createProgramPlan();
			this.millis = millis;
		}

		@Override
		public void run() {


			while (true) {
				try {
					exec.executePlan(plan);
					Thread.sleep(millis);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}


		}
	}

}
