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
        context().setReceiveTimeout(Duration.create(10, TimeUnit.SECONDS));
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(EventEnvelope.class, this::processEvent)
                .match(ReceiveTimeout.class, this::receiveTimeout)
                .build();
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

    private void deposit(EventDeposit eventDeposit, long offset) {
        log().info("Update {}, offset {}", eventDeposit, offset);
        // TODO update the query side
        // Assume that the query side persists the event log offset.
        // Assume that an update is conditionally performed by validating the offset.
        // Assume that the query side has primary key == entity identifier.
        // For example, update where offset < :envelope.offset
        sender().tell(String.format("Processed deposit %s %d", eventDeposit, offset), self());
    }

    private void withdrawal(EventWithdrawal eventWithdrawal, long offset) {
        log().info("Update {}, offset {}", eventWithdrawal, offset);
        // TODO update the query side
        // Assume that the query side persists the event log offset.
        // Assume that an update is conditionally performed by validating the offset.
        // Assume that the query side has primary key == entity identifier.
        // For example, update where offset < :envelope.offset
        sender().tell(String.format("Processed %s %d", eventWithdrawal, offset), self());
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
