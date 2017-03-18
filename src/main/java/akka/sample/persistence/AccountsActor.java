package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import scala.Option;

/**
 * The actor that provides access to accounts.
 *
 * <p>This actor delegates messages to specific {@link AccountPersistentActor} actors based on the account identifier.
 * If an instance of the {@link AccountPersistentActor} does not exist then one is created as a child actor and the
 * message is forwarded to the child. If the instance already exists then the message is forwarded to the child.</p>
 */
class AccountsActor extends AbstractLoggingActor {
    static Props props() {
        return Props.create(AccountsActor.class);
    }

    {
        receive(ReceiveBuilder
                .match(AccountPersistentActor.CommandDeposit.class, this::deposit)
                .match(AccountPersistentActor.CommandWithdrawal.class, this::withdrawal)
                .match(AccountPersistentActor.CommandGetAccount.class, this::getAccount)
                .build());
    }

    private void deposit(AccountPersistentActor.CommandDeposit deposit) {
        sendCommandToAccount(deposit.accountIdentifier(), deposit);
    }

    private void withdrawal(AccountPersistentActor.CommandWithdrawal withdrawal) {
        sendCommandToAccount(withdrawal.accountIdentifier(), withdrawal);
    }

    private void getAccount(AccountPersistentActor.CommandGetAccount commandGetAccount) {
        sendCommandToAccount(commandGetAccount.accountIdentifier(), commandGetAccount);
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
