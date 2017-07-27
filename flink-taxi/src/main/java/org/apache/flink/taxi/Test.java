package org.apache.flink.taxi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
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

public class Test {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {
		//FLINK CONFIGURATION
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		//MAIN PROGRAM
		//Read from Kafka
//		DataStream<String> line = env.addSource(myConsumer);
		DataStream<String> line = env.addSource(new StringSource(4100, 1));
//		DataStream<String> line = env.fromCollection(generateCollection())
//				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
//					@Nullable
//					@Override
//					public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
//						return new Watermark(extractedTimestamp - 10);
//					}
//
//					@Override
//					public long extractTimestamp(String element, long previousElementTimestamp) {
//						return Long.valueOf(element.split(" ")[1]);
//					}
//				});

		//Add 1 to each line
		DataStream<Tuple2<String, Integer>> line_Num = line.map(new NumberAdder());

		//Filted Odd numbers
		DataStream<Tuple2<String, Integer>> line_Num_Odd = line_Num.filter(new FilterOdd());

		//Filter Even numbers
		DataStream<Tuple2<String, Integer>> line_Num_Even = line_Num.filter(new FilterEven());

		//Join Even and Odd
		DataStream<Tuple2<String, Integer>> line_Num_U = line_Num_Odd.union(line_Num_Even);
//		line_Num_U.print();

		//Tumbling windows every 2 seconds
		AllWindowedStream<Tuple2<String, Integer>, TimeWindow> windowedLine_Num_U = line_Num_U
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2)));
//				.windowAll(TumblingEventTimeWindows.of(Time.seconds(2)));

		//Reduce to one line with the sum
		DataStream<Tuple2<String, Integer>> wL_Num_U_Reduced = windowedLine_Num_U.reduce(new Reducer());

		//Calculate the average of the elements summed
		DataStream<String> wL_Average = wL_Num_U_Reduced.map(new AverageCalculator());

		//Add timestamp and calculate the difference with the average
		DataStream<String> averageTS = wL_Average.map(new TimestampAdder());

		averageTS.print();

		env.execute("TimestampLongKafka");
	}

	private static Collection<String> generateCollection() {
		List<String> result = new ArrayList<>();
		for (long i = 1; i < 10000; i++) {
			result.add(String.format("%d %d", i, i + 1497790546981L));
		}
		return result;
	}


//Functions used in the program implementation:

	public static class FilterOdd implements FilterFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public boolean filter(Tuple2<String, Integer> line) throws Exception {
			Boolean isOdd = (Long.valueOf(line.f0.split(" ")[0]) % 2) != 0;
			return isOdd;
		}
	};


	public static class FilterEven implements FilterFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public boolean filter(Tuple2<String, Integer> line) throws Exception {
			Boolean isEven = (Long.valueOf(line.f0.split(" ")[0]) % 2) == 0;
			return isEven;
		}
	};


	public static class NumberAdder implements MapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> map(String line) {
			Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(line, 1);
			return newLine;
		}
	};


	public static class Reducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> line1, Tuple2<String, Integer> line2) throws Exception {
			Long sum = Long.valueOf(line1.f0.split(" ")[0]) + Long.valueOf(line2.f0.split(" ")[0]);
			Long sumTS = Long.valueOf(line1.f0.split(" ")[1]) + Long.valueOf(line2.f0.split(" ")[1]);
			Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(String.valueOf(sum) + " " + String.valueOf(sumTS),
					line1.f1 + line2.f1);
			return newLine;
		}
	};


	public static class AverageCalculator implements MapFunction<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		public String map(Tuple2<String, Integer> line) throws Exception {
			Long average = Long.valueOf(line.f0.split(" ")[1]) / line.f1;
			String result = String.valueOf(line.f1) + " " + String.valueOf(average);
			return result;
		}
	};


	public static final class TimestampAdder implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public String map(String line) throws Exception {
			Long currentTime = System.currentTimeMillis();
			String totalTime = String.valueOf(currentTime - Long.valueOf(line.split(" ")[1]));
			String newLine = line.concat(" " + String.valueOf(currentTime) + " " + totalTime);

			return newLine;
		}
	};
}
