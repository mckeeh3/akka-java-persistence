package akka.sample.persistence;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.PersistenceQuery;
import akka.sample.persistence.AccountWriteSide.EventDeposit;
import akka.sample.persistence.AccountWriteSide.EventWithdrawal;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import scala.Option;

import java.util.concurrent.CompletionStage;

import static akka.pattern.PatternsCS.ask;

/**
 * Process account events on the read side.
 */
class AccountsReadSide extends AbstractLoggingActor {
    {
        final ActorMaterializer materializer = ActorMaterializer.create(context().system());
        final CassandraReadJournal readJournal = cassandraReadJournal(context().system());

        readJournal
                .eventsByTag("account", 0L) // TODO need to start from a know offset
                .mapAsync(5, eventEnvelope -> processEvent(eventEnvelope, self()))
                .runWith(Sink.ignore(), materializer);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(EventEnvelope.class, this::taggedEvent)
                .matchAny(this::unhandled)
                .build();
    }

    private void taggedEvent(EventEnvelope eventEnvelope) {
        sendCommandToAccount(accountIdentifier(eventEnvelope), eventEnvelope);
    }

    private AccountIdentifier accountIdentifier(EventEnvelope eventEnvelope) {
        if (eventEnvelope.event() instanceof EventDeposit) {
            return ((EventDeposit) eventEnvelope.event()).accountIdentifier();
        }
        else if (eventEnvelope.event() instanceof EventWithdrawal) {
            return ((EventWithdrawal) eventEnvelope.event()).accountIdentifier();
        }
        else {
            return null;
        }
    }

    @Override
    public void unhandled(Object message) {
        log().info("Unhandled {}", message);
        super.unhandled(message);
    }

    private void sendCommandToAccount(AccountIdentifier accountIdentifier, Object message) {
        Option<ActorRef> accountRefOption = context().child(accountIdentifier.identifier());
        if (accountRefOption.isDefined()) {
            accountRefOption.get().forward(message, context());
        }
        else {
            ActorRef accountRef = context().actorOf(AccountReadSide.props(accountIdentifier), accountIdentifier.identifier());
            accountRef.forward(message, context());
        }
    }

    private CompletionStage<EventEnvelope> processEvent(EventEnvelope eventEnvelope, ActorRef a) {
        CompletionStage<Object> f = ask(a, eventEnvelope, 10L);
        return f.thenApplyAsync(e -> eventEnvelope);
    }

    private CassandraReadJournal cassandraReadJournal(ActorSystem actorSystem) {
        return PersistenceQuery.get(actorSystem)
                .getReadJournalFor(CassandraReadJournal.class, CassandraReadJournal.Identifier());
    }

    static Props props() {
        return Props.create(AccountsReadSide.class);
    }
}
