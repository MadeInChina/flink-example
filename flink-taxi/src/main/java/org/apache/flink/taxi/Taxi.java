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
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class Taxi {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		int maxDelay = 60;
		int servingSpeed = 600;

		// get an ExecutionEnvironment
		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource("/Users/pnowojski/flink-example/taxi/nycTaxiRides.gz", maxDelay, servingSpeed));

		DataStream<TaxiRide> filteredRides = rides.filter(new NycRidesFilter());

		filteredRides
				.map(new MapFunction<TaxiRide, String>() {
					@Override
					public String map(TaxiRide taxiRide) throws Exception {
						return taxiRide.toString();
					}
				})
				.addSink(
				new FlinkKafkaProducer010<String>(
						"localhost:9092",
						"nycRides",
						new SimpleStringSchema()
				)
		);

		env.execute("Taxi");
	}

}
