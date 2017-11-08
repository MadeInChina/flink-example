package org.apache.flink.quickstart;

import org.apache.flink.quickstart.akka.CalculatorActor;
import org.apache.flink.quickstart.akka.LookupActor;
import org.apache.flink.quickstart.akka.Op;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.ConfigFactory;

import java.util.Random;

import scala.concurrent.duration.Duration;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CalculatorApplication {
  public static void main(String[] args) {
      startRemoteCalculatorSystem();
  }

  public static void startRemoteCalculatorSystem() {
    final ActorSystem system = ActorSystem.create("CalculatorSystem",
        ConfigFactory.load(("calculator")));
    system.actorOf(Props.create(CalculatorActor.class), "calculator");
    System.out.println("Started CalculatorSystem");
  }
}
