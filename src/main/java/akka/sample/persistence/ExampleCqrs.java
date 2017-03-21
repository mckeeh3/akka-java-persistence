package akka.sample.persistence;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.PersistenceQuery;
import akka.stream.ActorMaterializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

import static akka.pattern.PatternsCS.ask;

/**
 * Playing with Akka persistence query.
 */
public class ExampleCqrs {
    private static final Logger log = LoggerFactory.getLogger(ExampleCqrs.class);

    {
        ActorSystem actorSystem = ActorSystem.create("cqrs");

        final ActorMaterializer materializer = ActorMaterializer.create(actorSystem);

        CassandraReadJournal readJournal = cassandraReadJournal(actorSystem);
        //LeveldbReadJournal readJournal = leveldbReadJournal(actorSystem);


        readJournal
                .allPersistenceIds()
                .runForeach(System.out::println, materializer);

        ActorRef actor = actorSystem.actorOf(AnActor.props());

        readJournal
                .eventsByTag("account", 0L)
                .mapAsync(5, eventEnvelope -> processEvent(eventEnvelope, actor))
                .mapAsync(1, this::saveOffset)
                .runForeach(this::logIdentifier, materializer);
    }

//    private LeveldbReadJournal leveldbReadJournal(ActorSystem actorSystem) {
//        return PersistenceQuery.get(actorSystem)
//                .getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());
//    }

    private CassandraReadJournal cassandraReadJournal(ActorSystem actorSystem) {
        return PersistenceQuery.get(actorSystem)
                .getReadJournalFor(CassandraReadJournal.class, CassandraReadJournal.Identifier());
    }

    private CompletionStage<EventEnvelope> processEvent(EventEnvelope eventEnvelope) {
        return null;
    }

    private CompletionStage<Long> saveOffset(EventEnvelope eventEnvelope) {
        return null;
    }

    private CompletionStage<EventEnvelope> processEvent(EventEnvelope eventEnvelope, ActorRef a) {
        CompletionStage<Object> f = ask(a, eventEnvelope, 10L);
        return f.thenApplyAsync(e -> eventEnvelope);
    }

    private void logIdentifier(long identifier) {
        log.info("Identifier {}", identifier);
    }

    public static void main(String[] arguments) {
        new ExampleCqrs();
    }

    private static class AnActor extends AbstractActor {
        private final LoggingAdapter log = Logging.getLogger(context().system(), this);

        {
            receive(ReceiveBuilder
                    .matchAny(this::handle)
                    .build());
        }

        private void handle(Object message) {
            System.out.println(message);
            log.info("{} {}", message, sender());
        }

        static Props props() {
            return Props.create(AnActor.class);
        }
    }
}
