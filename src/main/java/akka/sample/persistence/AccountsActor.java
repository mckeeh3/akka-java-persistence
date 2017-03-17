package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import scala.Option;

/**
 * The actor that provides access to accounts.
 */
class AccountsActor extends AbstractLoggingActor {
    static Props props() {
        return Props.create(AccountsActor.class);
    }

    {
        receive(ReceiveBuilder
                .match(CommandDeposit.class, this::deposit)
                .match(CommandWithdrawal.class, this::withdrawal)
                .build());
    }

    private void deposit(CommandDeposit deposit) {
        sendCommandToAccount(deposit.accountIdentifier(), deposit);
    }

    private void withdrawal(CommandWithdrawal withdrawal) {
        sendCommandToAccount(withdrawal.accountIdentifier(), withdrawal);
    }

    private void sendCommandToAccount(AccountIdentifier accountIdentifier, Object message) {
        Option<ActorRef> accountRefOption = context().child(accountIdentifier.identifier());
        if (accountRefOption.isDefined()) {
            accountRefOption.get().forward(message, context());
        } else {
            ActorRef accountRef = context().actorOf(AccountPersistentActor.props(accountIdentifier), accountIdentifier.identifier());
            accountRef.forward(message, context());
        }
    }
}
