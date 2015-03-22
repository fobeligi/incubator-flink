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

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;
import org.apache.flink.client.LocalExecutor;


public class LambdaTriggeredJoin {

	private static final Logger log = LoggerFactory.getLogger(LambdaTriggeredJoin.class);

	private static final String JAR = "/home/fobeligi/workspace/incubator-flink/flink-staging/flink-streaming/flink-streaming-examples/target/flink-streaming-examples-0.9-SNAPSHOT-LambdaTriggeredJoin.jar";

	public static LocalExecutor exec = new LocalExecutor(false);


	public static void main(String[] args) throws Exception {

		exec.setTaskManagerNumSlots(4);
		exec.start();

		try {
			BatchJob bj = new BatchJob(JAR, exec);
			new Thread(bj).start();
			new Thread(new StreamingJob(JAR, bj.getDataSet())).start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			exec.stop();
		}

	}

}
