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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.FileMonitoringFunction;


public class StreamingJob implements Runnable {

    StreamExecutionEnvironment streamEnv;
    DataSet dataSet;

    public StreamingJob(String JAR, DataSet ds) {
        this.streamEnv = StreamExecutionEnvironment.createRemoteEnvironment(
                "127.0.0.1", 6123, 1, JAR);
        this.dataSet = ds;
    }

    @Override
    public void run() {

        DataStream<Tuple2<String, Integer>> dataSetStream = streamEnv.readFileStream(
                "file:///home/fobeligi/FlinkTmp/temp2", dataSet.getType(), 1000,
                FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED);

        dataSetStream.print();
//		dataSetStream.project(1).types(Integer.class).print();
        try {
            streamEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
