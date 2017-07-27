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
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.joda.time.Minutes;
import org.joda.time.Seconds;

import static com.google.common.base.Preconditions.checkNotNull;

public class TravelTimePrediction {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		int servingSpeed = 60;

		// get an ExecutionEnvironment
		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(1000);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(6, Time.seconds(10)));

		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(
				new CheckpointedTaxiRideSource("/Users/pnowojski/flink-example/taxi/nycTaxiRides.gz", servingSpeed));


		rides.filter(new NycRidesFilter())
				.keyBy(new KeySelector<TaxiRide, Integer>() {
					@Override
					public Integer getKey(TaxiRide taxiRide) throws Exception {
						return GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
					}
				})
				.flatMap(new TimeTravelPredictor())
				.filter(new FilterFunction<Tuple2<Long, Integer>>() {
					@Override
					public boolean filter(Tuple2<Long, Integer> predictions) throws Exception {
						return predictions.f1 > 0;
					}
				})
				.print();

		env.execute("Taxi");
	}

	private static class TimeTravelPredictor extends RichFlatMapFunction<TaxiRide, Tuple2<Long, Integer>> {

		private ValueState<TravelTimePredictionModel> predictionModel;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<TravelTimePredictionModel> descriptor = new ValueStateDescriptor<>(
					"predictionModel",
					TravelTimePredictionModel.class,
					new TravelTimePredictionModel());
			predictionModel = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Long, Integer>> collector) throws Exception {
			TravelTimePredictionModel model = predictionModel.value();

			int direction = GeoUtils.getDirectionAngle(taxiRide.startLon, taxiRide.startLat, taxiRide.endLon, taxiRide.endLat);
			double distance = GeoUtils.getEuclideanDistance(taxiRide.startLon, taxiRide.startLat, taxiRide.endLon, taxiRide.endLat);
			if (taxiRide.isStart) {
				collector.collect(Tuple2.of(taxiRide.rideId, model.predictTravelTime(direction, distance)));
			}
			else {
				checkNotNull(taxiRide.endTime);
				checkNotNull(taxiRide.startTime);
				long millis = taxiRide.endTime.getMillis() - taxiRide.startTime.getMillis();
				double minutes = ((double) millis) / (60 * 1000);
				model.refineModel(direction, distance, minutes);
				predictionModel.update(model);
			}
		}
	}
}
