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

package org.apache.flink.streaming.examples.namedstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class NamedStream<T> {
	
	//Create a NamedStream from an appropriate DataStream
	public NamedStream(DataStream<Tuple2<T, String>> ds) {

	}
	
	//Process the elements using a user defined flatmapfunction which which for each input of type
	// T returns collects a number of outputs of type R with the name of the output wrapped ina  Tuple2<R, String>
	public <R> NamedStream<R> process(FlatMapFunction<T, Tuple2<R, String>> function) {
		return null;
	}

	//Converts the NamedStream back to a regular stream by dropping the name field
	//Tip: Instead of projection use a regular map
	public DataStream<T> toDataStream() {
		return null;
	}

	//Creates a new NamedStream by merging substreams(defined by name) of the given NamedStreams 
	public static <X> NamedStream<X> mergeByName(String name, NamedStream<X>... namedStreams) {
		return null;
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(2);

		DataStream<Tuple2<Long, String>> stream1 = env.generateSequence(1, 10)
				.map(new WithNames());
		DataStream<Tuple2<Long, String>> stream2 = env.generateSequence(11, 20)
				.map(new WithNames());

		NamedStream<Long> named1 = new NamedStream<Long>(stream1);
		NamedStream<Long> named2 = new NamedStream<Long>(stream2);

		NamedStream<Long> processed1 = named1.process(new IncrementWithNames());
		NamedStream<Long> processed2 = named2.process(new IncrementWithNames());

		NamedStream.mergeByName("even", processed1, processed2).toDataStream().print();

	}	

	private static class WithNames implements MapFunction<Long, Tuple2<Long, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, String> map(Long value) throws Exception {
			return new Tuple2<Long, String>(value, value % 2 == 0 ? "even" : "odd");
		}
	}

	private static class IncrementWithNames implements FlatMapFunction<Long, Tuple2<Long, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Long value, Collector<Tuple2<Long, String>> out) throws Exception {

			Long incremented = value++;
			String outputName = incremented % 2 == 0 ? "even" : "odd";

			out.collect(new Tuple2<Long, String>(incremented, outputName));

		}
	}
}
