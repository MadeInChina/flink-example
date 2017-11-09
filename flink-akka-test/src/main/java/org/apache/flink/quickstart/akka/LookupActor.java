package org.apache.flink.quickstart.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.actor.ReceiveTimeout;
import akka.actor.Terminated;

import scala.concurrent.duration.Duration;

import static java.util.concurrent.TimeUnit.SECONDS;

public class LookupActor extends AbstractActor {

  private final String path;
  private ActorRef calculator = null;

  public LookupActor(String path) {
    this.path = path;
    sendIdentifyRequest();
  }

  private void sendIdentifyRequest() {
    getContext().actorSelection(path).tell(new Identify(path), self());
    getContext()
        .system()
        .scheduler()
        .scheduleOnce(Duration.create(3, SECONDS), self(),
            ReceiveTimeout.getInstance(), getContext().dispatcher(), self());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(ActorIdentity.class, identity -> {
        calculator = identity.getRef();
        if (calculator == null) {
          System.out.println("Remote actor not available: " + path);
        } else {
          getContext().watch(calculator);
          getContext().become(active, true);
        }
      })
      .match(ReceiveTimeout.class, x -> {
        sendIdentifyRequest();
      })
      .build();
  }

  Receive active = receiveBuilder()
    .match(Op.MathOp.class, message -> {
      // send message to server actor
      calculator.tell(message, self());
    })
    .match(Op.AddResult.class, result -> {
      System.out.printf("Add result: %d + %d = %d\n", result.getN1(),
        result.getN2(), result.getResult());
    })
    .match(Op.MultiplicationResult.class, result -> {
      System.out.printf("Multiplication result: %d * %d = %d\n", result.getN1(),
              result.getN2(), result.getResult());
    })
    .match(Op.SubtractResult.class, result -> {
      System.out.printf("Sub result: %d - %d = %d\n", result.getN1(),
        result.getN2(), result.getResult());
    })
    .match(Terminated.class, terminated -> {
      System.out.println("Calculator terminated");
      sendIdentifyRequest();
      getContext().unbecome();
    })
    .match(ReceiveTimeout.class, message -> {
      // ignore
    })
    .build();

}
