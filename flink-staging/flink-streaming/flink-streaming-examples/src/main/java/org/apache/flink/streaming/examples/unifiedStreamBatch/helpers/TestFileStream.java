package org.apache.flink.streaming.examples.unifiedStreamBatch.helpers;/*
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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;

public class TestFileStream {


	public static void main(String[] args) throws Exception {


		StreamExecutionEnvironment streamEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> dataSetStream = streamEnvironment.readFileStream("file:///Users/fobeligi/workspace/master-thesis/dataSets/FlinkTmp/",
				1000, FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED);

		dataSetStream.print();

		streamEnvironment.execute();

//		ExecutionEnvironment batchEnvironment = ExecutionEnvironment.getExecutionEnvironment();
//		CsvReader csvR = batchEnvironment.readCsvFile("/Users/fobeligi/workspace/master-thesis/dataSets/UnifiedBatchStream.csv");
//		csvR.lineDelimiter("\n").fieldDelimiter(",");
//		DataSet<Tuple2<Double, Integer>> batchDataSet = csvR.types(Double.class, Integer.class);
//
//		batchDataSet.write(new TypeSerializerOutputFormat<Tuple2<Double, Integer>>(), "/Users/fobeligi/workspace/master-thesis/dataSets/FlinkTmp/",
//				FileSystem.WriteMode.OVERWRITE);
//
//		batchDataSet.print();
	}
}
