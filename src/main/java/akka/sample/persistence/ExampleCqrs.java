package akka.sample.persistence;

import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorContext;
import akka.actor.ActorSystem;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.javadsl.ReadJournal;
import akka.persistence.query.journal.leveldb.javadsl.LeveldbReadJournal;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;

/**
 * Playing with Akka persistence query.
 */
public class ExampleCqrs {
    public static void main(String[] arguments) {
        ActorSystem actorSystem = ActorSystem.create("cqrs");

        final ActorMaterializer mat = ActorMaterializer.create(actorSystem);

        LeveldbReadJournal queries = PersistenceQuery.get(actorSystem)
                .getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());

        Source<String, NotUsed> source = queries.eventsallPersistenceIds();
        source.runForeach(event -> System.out.println(event), mat);
    }

    private static class AccountReadJournal implements ReadJournal {

    }

    private static class AnActor extends AbstractActor {
        {
            context().system().settings().config().getString("");
        }
    }
}
