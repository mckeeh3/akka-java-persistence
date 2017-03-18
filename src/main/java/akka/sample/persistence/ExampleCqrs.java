package akka.sample.persistence;

import akka.actor.ActorSystem;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.javadsl.ReadJournal;

/**
 * Playing with Akka persistence query.
 */
public class ExampleCqrs {
    public static void main(String[] arguments) {
        ActorSystem actorSystem = ActorSystem.create("cqrs");
        PersistenceQuery.get(actorSystem).getReadJournalFor(AccountReadJournal.class, "");
    }

    private static class AccountReadJournal implements ReadJournal {

    }
}
