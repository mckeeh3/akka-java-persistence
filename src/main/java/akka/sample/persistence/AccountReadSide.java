package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.japi.pf.ReceiveBuilder;
import akka.sample.persistence.AccountWriteSide.EventDeposit;
import akka.sample.persistence.AccountWriteSide.EventWithdrawal;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Process account events on the read side.
 */
public class AccountReadSide extends AbstractLoggingActor {
    private final AccountIdentifier accountIdentifier;

    {
        receive(ReceiveBuilder
                .match(EventDeposit.class, this::deposit)
                .match(EventWithdrawal.class, this::withdrawal)
                .match(ReceiveTimeout.class, this::receiveTimeout)
                .build());

        context().setReceiveTimeout(Duration.create(10, TimeUnit.SECONDS));
    }

    public AccountReadSide(AccountIdentifier accountIdentifier) {
        this.accountIdentifier = accountIdentifier;
    }

    private void deposit(EventDeposit eventDeposit) {
        log().info("Update {}", eventDeposit);
        // TODO update the query side
    }

    private void withdrawal(EventWithdrawal eventWithdrawal) {
        log().info("Update {}", eventWithdrawal);
        // TODO update the query side
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

    private void receiveTimeout(ReceiveTimeout receiveTimeout) {
        log().info("Idle timeout {}, {} timeout", accountIdentifier, receiveTimeout);
        context().stop(self());
    }

    static Props props(AccountIdentifier accountIdentifier) {
        return Props.create(AccountReadSide.class, accountIdentifier);
    }
}
