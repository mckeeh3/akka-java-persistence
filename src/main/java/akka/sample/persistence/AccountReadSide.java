package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.query.EventEnvelope;
import akka.sample.persistence.AccountWriteSide.EventDeposit;
import akka.sample.persistence.AccountWriteSide.EventWithdrawal;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Process account events on the read side.
 */
class AccountReadSide extends AbstractLoggingActor {
    private final AccountIdentifier accountIdentifier;

    {
        receive(ReceiveBuilder
                .match(EventEnvelope.class, this::processEvent)
                .match(ReceiveTimeout.class, this::receiveTimeout)
                .build());

        context().setReceiveTimeout(Duration.create(10, TimeUnit.SECONDS));
    }

    public AccountReadSide(AccountIdentifier accountIdentifier) {
        this.accountIdentifier = accountIdentifier;
    }

    private void processEvent(EventEnvelope eventEnvelope) {
        if (isDeposit(eventEnvelope)) {
            deposit((EventDeposit) eventEnvelope.event(), eventEnvelope.sequenceNr());
        }
        else if (isWithdrawal(eventEnvelope)) {
            withdrawal((EventWithdrawal) eventEnvelope.event(), eventEnvelope.sequenceNr());
        }
        else {
            notProcessed(eventEnvelope);
        }
    }

    private void deposit(EventDeposit eventDeposit, long sequenceNr) {
        log().info("Update {}, sequence {}", eventDeposit, sequenceNr);
        // TODO update the query side
        // Assume that the query side persists the event log sequence.
        // Assume that an update is conditionally performed by validating the sequence.
        // Assume that the query side has primary key == entity identifier.
        // For example, update where sequence < :envelope.sequence
        sender().tell(String.format("Processed deposit %s %d", eventDeposit, sequenceNr), self());
    }

    private void withdrawal(EventWithdrawal eventWithdrawal, long sequenceNr) {
        log().info("Update {}, sequence {}", eventWithdrawal, sequenceNr);
        // TODO update the query side
        // Assume that the query side persists the event log sequence.
        // Assume that an update is conditionally performed by validating the sequence.
        // Assume that the query side has primary key == entity identifier.
        // For example, update where sequence < :envelope.sequence
        sender().tell(String.format("Processed %s %d", eventWithdrawal, sequenceNr), self());
    }

    private void notProcessed(EventEnvelope eventEnvelope) {
        log().info("Rejected {}", eventEnvelope);
        sender().tell(String.format("Not processed %s", eventEnvelope), self());
    }

    @Override
    public void preStart() throws Exception {
        log().info("Start {}", accountIdentifier);
    }

    @Override
    public void postStop() throws Exception {
        context().setReceiveTimeout(Duration.Undefined());
        log().info("Stop {}", accountIdentifier);
    }

    private boolean isDeposit(EventEnvelope eventEnvelope) {
        return eventEnvelope.event() instanceof EventDeposit;
    }

    private boolean isWithdrawal(EventEnvelope eventEnvelope) {
        return eventEnvelope.event() instanceof EventWithdrawal;
    }

    private void receiveTimeout(ReceiveTimeout receiveTimeout) {
        log().info("Idle timeout {}, {} timeout", accountIdentifier, receiveTimeout);
        context().stop(self());
    }

    static Props props(AccountIdentifier accountIdentifier) {
        return Props.create(AccountReadSide.class, accountIdentifier);
    }
}
