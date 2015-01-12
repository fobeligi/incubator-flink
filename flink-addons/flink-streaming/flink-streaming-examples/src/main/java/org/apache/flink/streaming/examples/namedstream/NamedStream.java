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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class NamedStream<T> {
	
	protected SingleOutputStreamOperator<Tuple2<T,String>, ?> ns;
	
	
	public NamedStream(SingleOutputStreamOperator<Tuple2<T,String>,?> ds){
		this.ns = ds.copy();
	}
	
	//Process the elements using a user defined flatmapfunction which for each input of type
	// T returns collects a number of outputs of type R with the name of the output wrapped in a Tuple2<R, String>
	public <R> NamedStream<R> process(FlatMapFunction<T, Tuple2<R, String>> function) {
		SingleOutputStreamOperator<Tuple2<R, String>, ?> nstr = this.toDataStream().flatMap(function);
		return new NamedStream<R>(nstr);
	}

	//Converts the NamedStream back to a regular stream by dropping the name field
	//Tip: Instead of projection use a regular map
	public DataStream<T> toDataStream() {
		return this.ns.map(new MapFunction<Tuple2<T,String>,T>() {
			private static final long serialVersionUID = 1L;
			@Override
			public T map(Tuple2<T, String> value) throws Exception {
				return value.f0;
			}
        });
	}

	//Creates a new NamedStream by merging substreams(defined by name) of the given NamedStreams 
	@SuppressWarnings("unchecked")
	public static <X> NamedStream<X> mergeByName(String name, NamedStream<X>... namedStreams) {
		List< DataStream< Tuple2< X, String>>> sel = new ArrayList< DataStream< Tuple2< X, String>>>();
		
		for (NamedStream<X> stream : namedStreams) {		 
			SplitDataStream<Tuple2<X, String>> selStream  = stream.ns.split(new OutputSelector<Tuple2<X, String>>() {
				private static final long serialVersionUID = 1L;

				@Override
	            public Iterable<String> select(Tuple2<X, String> ist2) {
	                List<String> outputs = new ArrayList<String>();
	                outputs.add(ist2.getField(1).toString());
	                return outputs;
	            }
	        });
			
			sel.add(selStream.select(name));
		}
		if (sel.size()==0){
			throw new RuntimeException("At least one NamedStream should be passed as an argument.");
		}
		DataStream<Tuple2<X, String>> ds = sel.get(0);
		if (sel.size()>1){
			ds = ds.merge(sel.subList(1, sel.size()).toArray(new DataStream[0]));
		}
		return new NamedStream<X> ((SingleOutputStreamOperator<Tuple2<X, String>, ?>) ds);
			
	}
		
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(2);
		
		SingleOutputStreamOperator<Tuple2<Long,String>,?> s1 = env.generateSequence(1, 10)
				.map(new WithNames());
		SingleOutputStreamOperator<Tuple2<Long,String>,?> s2 = env.generateSequence(11, 20)
				.map(new WithNames());

		NamedStream<Long> named1 = new NamedStream<Long>(s1);
		NamedStream<Long> named2 = new NamedStream<Long>(s2);

		NamedStream<Long> processed1 = named1.process(new IncrementWithNames());
		NamedStream<Long> processed2 = named2.process(new IncrementWithNames());
		
		NamedStream.mergeByName("odd", processed1, processed2).toDataStream().print();
		
		env.execute();
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
			Long incremented = ++value;
			String outputName = incremented % 2 == 0 ? "even" : "odd";

			out.collect(new Tuple2<Long, String>(incremented, outputName));

		}
	}
}
