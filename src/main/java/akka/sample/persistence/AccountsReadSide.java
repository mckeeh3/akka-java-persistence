package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import akka.sample.persistence.AccountWriteSide.EventDeposit;
import akka.sample.persistence.AccountWriteSide.EventWithdrawal;
import scala.Option;

/**
 * Process account events on the read side.
 */
public class AccountsReadSide extends AbstractLoggingActor {
    {
        receive(ReceiveBuilder
                .match(EventDeposit.class, this::deposit)
                .match(EventWithdrawal.class, this::withdrawal)
                .build());
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
}
