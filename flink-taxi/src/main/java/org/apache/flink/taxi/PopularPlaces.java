package org.apache.flink.taxi;

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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Properties;

public class PopularPlaces {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		final int maxDelay = 60;
		final int popularityThreshold = 20;

		// get an ExecutionEnvironment
		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get the taxi ride data stream
//		DataStream<TaxiRide> rides = env.addSource(
//				new TaxiRideSource("/Users/pnowojski/flink-example/taxi/nycTaxiRides.gz", maxDelay, servingSpeed));

//		DataStream<TaxiRide> nycRides = rides.filter(new FilterFunction<TaxiRide>() {
//			@Override
//			public boolean filter(TaxiRide taxiRide) throws Exception {
//				return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
//			}
//		});

		Properties properties = new Properties();
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "PopularPlaces");
		DataStream<TaxiRide> nycRides = env.addSource(
				new FlinkKafkaConsumer010<String>(
						"nycRides",
						new SimpleStringSchema(),
						properties))
				.map(new MapFunction<String, TaxiRide>() {
					@Override
					public TaxiRide map(String line) throws Exception {
						return TaxiRide.fromString(line);
					}})
				.assignTimestampsAndWatermarks(new TaxiRideTimestampAssigner(maxDelay));

		//lon, lat, ts, isStart, count
		DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> result = nycRides
				.keyBy(new GridCellKeySelector())
				.timeWindow(Time.minutes(15), Time.minutes(5))
				.allowedLateness(Time.seconds(maxDelay))
				.apply(new CountRides())
				.filter(new FilterFunction<Tuple5<Float, Float, Long, Boolean, Integer>>() {
					@Override
					public boolean filter(Tuple5<Float, Float, Long, Boolean, Integer> entry) throws Exception {
						return entry.f4 >= popularityThreshold;
					}
				});

		result.print();

		env.execute("Taxi");
	}

	private static class CountRides implements WindowFunction<
                TaxiRide,
                Tuple5<Float, Float, Long, Boolean, Integer>,
				Tuple2<Integer, Boolean>,
                TimeWindow> {
		@Override
		public void apply(
				Tuple2<Integer, Boolean> key,
				TimeWindow timeWindow,
				Iterable<TaxiRide> events,
				Collector<Tuple5<Float, Float, Long, Boolean, Integer>> output) {
			int count = 0;
			for (TaxiRide event : events) {
				count++;
			}
			output.collect(Tuple5.of(
					GeoUtils.getGridCellCenterLon(key.f0),
					GeoUtils.getGridCellCenterLat(key.f0),
					timeWindow.getEnd(),
					key.f1,
					count));
		}
	}

	private static class TaxiRideTimestampAssigner implements AssignerWithPunctuatedWatermarks<TaxiRide> {
		private final int maxDelay;

		public TaxiRideTimestampAssigner(int maxDelay) {
			this.maxDelay = maxDelay;
		}

		@Nullable
        @Override
        public Watermark checkAndGetNextWatermark(TaxiRide taxiRide, long extractedTimestamp) {
            return new Watermark(extractedTimestamp - maxDelay);
        }

		@Override
        public long extractTimestamp(TaxiRide ride, long previousTimestamp) {
            if (ride.isStart) {
                return ride.startTime.getMillis();
            }
            else {
                return ride.endTime.getMillis();
            }
        }
	}
}
