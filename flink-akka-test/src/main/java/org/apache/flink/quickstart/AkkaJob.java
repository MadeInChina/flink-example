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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.quickstart.akka.LookupActor;
import org.apache.flink.quickstart.akka.Op;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AkkaJob {

	/**
	 * This requires started {@link CalculatorApplication}
	 */
	public static void main(String[] args) throws Exception {
//		startRemoteCalculatorSystem();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
				.transform("akka calculator lookup", BasicTypeInfo.INT_TYPE_INFO, new AkkaCalculatorLookup())
				.print();

		env.execute("Flink custom akka version test");
	}

	private static class AkkaCalculatorLookup extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		private transient ActorSystem system;
		private transient ActorRef actor;

		@Override
		public void open() throws Exception {
			Config config = ConfigFactory.load("remotelookup");
			system = ActorSystem.create("LookupSystem", config);
			actor = system.actorOf(
					Props.create(
							LookupActor.class,
							"akka.tcp://CalculatorSystem@127.0.0.1:2552/user/calculator"), "lookupActor");

			System.out.println("Started LookupSystem");
//			system.scheduler().schedule(Duration.create(1, SECONDS),
//					Duration.create(1, SECONDS), new Runnable() {
//						@Override
//						public void run() {
//							if (r.nextInt(100) % 2 == 0) {
//								actor.tell(new Op.Add(r.nextInt(100), r.nextInt(100)), null);
//							} else {
//								actor.tell(new Op.Subtract(r.nextInt(100), r.nextInt(100)), null);
//							}
//
//						}
//					}, system.dispatcher());

		}

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			Thread.sleep(100);

			actor.tell(new Op.Multiply(element.getValue(), element.getValue()), null);
			output.collect(element);
		}
	}
}
