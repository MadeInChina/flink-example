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
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public class CarEventSort {

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

		events
				.keyBy("carId")
				.process(new CarEventSorter())
				.print();

		env.execute();
	}

	private static class CarEventSorter extends RichProcessFunction<ConnectedCarEvent, ConnectedCarEvent> {
		public ValueState<PriorityQueue<ConnectedCarEvent>> carEventsState;

		@Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>> descriptor = new ValueStateDescriptor<>(
                    // state name
                    "sorted-events",
                    // type information of state
                    TypeInformation.of(new TypeHint<PriorityQueue<ConnectedCarEvent>>() {}),
					new PriorityQueue<ConnectedCarEvent>());
            carEventsState = getRuntimeContext().getState(descriptor);
        }

		@Override
        public void processElement(ConnectedCarEvent event, Context context, Collector<ConnectedCarEvent> out) throws Exception {
			TimerService timerService = context.timerService();
			if (context.timestamp() < timerService.currentWatermark()) {
				return;
			}
			PriorityQueue<ConnectedCarEvent> carEvents = carEventsState.value();
			carEvents.add(event);
			carEventsState.update(carEvents);
			timerService.registerEventTimeTimer(event.timestamp);
		}

		@Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<ConnectedCarEvent> out) throws Exception {
			PriorityQueue<ConnectedCarEvent> carEvents = carEventsState.value();
			ConnectedCarEvent head = carEvents.peek();

			while (head != null && head.timestamp <= context.timerService().currentWatermark()) {
				out.collect(head);
				carEvents.remove();
				head =  carEvents.peek();
			}
		}
	}
}
