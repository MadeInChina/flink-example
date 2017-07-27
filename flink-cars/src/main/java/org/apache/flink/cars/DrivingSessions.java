package org.apache.flink.cars;

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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.GapSegment;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DrivingSessions {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.get("input", "/Users/pnowojski/flink-example/cars/carOutOfOrder.csv");

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// connect to the data file
		DataStream<String> carData = env.readTextFile(input);

		// create event stream
		DataStream<ConnectedCarEvent> events = carData
				.map(new MapFunction<String, ConnectedCarEvent>() {
					@Override
					public ConnectedCarEvent map(String line) throws Exception {
						return ConnectedCarEvent.fromString(line);
					}
				})
				.assignTimestampsAndWatermarks(new ConnectedCarAssigner());

		IdentitiyMap identitiyMap = new IdentitiyMap();
		events
				.map(identitiyMap)
				.map(identitiyMap)
				.keyBy("carId")
				.window(EventTimeSessionWindows.withGap(Time.seconds(15)))
				.apply(new WindowFunction<ConnectedCarEvent, Object, Tuple, TimeWindow>() {
					@Override
					public void apply(Tuple tuple, TimeWindow window, Iterable<ConnectedCarEvent> input, Collector<Object> out) throws Exception {
						out.collect(new GapSegment(input));
					}
				})
				.print();

		env.execute();
	}

	private static class IdentitiyMap implements MapFunction<ConnectedCarEvent, ConnectedCarEvent> {
		private int state = 0;
		@Override
		public ConnectedCarEvent map(ConnectedCarEvent value) throws Exception {
			state += 1;
			return value;
		}
	}
}
