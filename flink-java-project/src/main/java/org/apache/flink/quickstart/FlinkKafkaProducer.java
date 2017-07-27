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

package org.apache.flink.quickstart;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;

import java.util.Properties;

public class FlinkKafkaProducer {

	public static void main(String[] args) throws Exception {

		if(args.length != 5){
			System.out.println("usage: topic num_elements parallelism restartAttempts restartDelayMs");
			return;
		}

		final String topic = args[0].toString();
		final int numElements = Integer.valueOf(args[1]);
		final int parallelism = Integer.valueOf(args[2]);
		final int restartAttempts = Integer.valueOf(args[3]);
		final long restartDelayMs = Long.valueOf(args[4]);

		TypeInformationSerializationSchema<String> schema = new TypeInformationSerializationSchema<>(BasicTypeInfo.STRING_TYPE_INFO, new ExecutionConfig());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, Time.milliseconds(restartDelayMs)));
//		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		Properties properties = new Properties();

		properties.put("bootstrap.servers", "localhost:9092");
		// decrease timeout and block time from 60s down to 10s - this is how long KafkaProducer will try send pending (not flushed) data on close()
		properties.setProperty("timeout.ms", "10000");
		properties.setProperty("max.block.ms", "10000");
		// increase batch.size and linger.ms - this tells KafkaProducer to batch produced events instead of flushing them immediately
		properties.setProperty("batch.size", "10240000");
		properties.setProperty("linger.ms", "10000");

		FlinkKafkaProducer010<String> producer = new FlinkKafkaProducer010<>(topic, schema, properties);
		producer.setFlushOnCheckpoint(true);
		// process exactly failAfterElements number of elements and then shutdown Kafka broker and fail application
		env
				.addSource(new IntegerSource(numElements))
				.map(new MapFunction<Integer, String>() {
					@Override
					public String map(Integer integer) throws Exception {
						return integer.toString();
					}
				})
				.addSink(producer);

		env.execute("FlinkKafkaProducer at-least-once test");
	}
}
