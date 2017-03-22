package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
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
public class AccountsReadSide extends AbstractLoggingActor {
    {
        final ActorMaterializer materializer = ActorMaterializer.create(context().system());
        final CassandraReadJournal readJournal = cassandraReadJournal(context().system());

        receive(ReceiveBuilder
                .match(EventDeposit.class, this::deposit)
                .match(EventWithdrawal.class, this::withdrawal)
                .build());

        readJournal
                .eventsByTag("account", 0L)
                .mapAsync(5, eventEnvelope -> processEvent(eventEnvelope, self()))
                .runWith(Sink.ignore(), materializer);
    }

    private void deposit(EventDeposit eventDeposit) {
        sendCommandToAccount(eventDeposit.accountIdentifier(), eventDeposit);
    }

    private void withdrawal(EventWithdrawal eventWithdrawal) {
        sendCommandToAccount(eventWithdrawal.accountIdentifier(), eventWithdrawal);
    }

    private void sendCommandToAccount(AccountIdentifier accountIdentifier, Object message) {
        Option<ActorRef> accountRefOption = context().child(accountIdentifier.identifier());
        if (accountRefOption.isDefined()) {
            accountRefOption.get().forward(message, context());
        } else {
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
}
