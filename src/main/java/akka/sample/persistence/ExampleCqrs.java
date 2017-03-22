package akka.sample.persistence;

import akka.actor.ActorSystem;

/**
 * Playing with Akka persistence query.
 */
public class ExampleCqrs {
    {
        ActorSystem actorSystem = ActorSystem.create("cqrs");
        actorSystem.actorOf(AccountsReadSide.props(), "accounts-read-side");
    }

    public static void main(String[] arguments) {
        new ExampleCqrs();
    }
}
