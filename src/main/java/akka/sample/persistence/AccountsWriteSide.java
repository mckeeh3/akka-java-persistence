package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import scala.Option;

/**
 * The actor that provides access to accounts.
 * <p>
 * <p>This actor delegates messages to specific {@link AccountWriteSide} actors based on the account identifier.
 * If an instance of the {@link AccountWriteSide} does not exist then one is created as a child actor and the
 * message is forwarded to the child. If the instance already exists then the message is forwarded to the child.</p>
 */
class AccountsWriteSide extends AbstractLoggingActor {
    static Props props() {
        return Props.create(AccountsWriteSide.class);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(AccountWriteSide.CommandDeposit.class, this::deposit)
                .match(AccountWriteSide.CommandWithdrawal.class, this::withdrawal)
                .match(AccountWriteSide.CommandGetAccount.class, this::getAccount)
                .build();
    }

    private void deposit(AccountWriteSide.CommandDeposit deposit) {
        sendCommandToAccount(deposit.accountIdentifier(), deposit);
    }

    private void withdrawal(AccountWriteSide.CommandWithdrawal withdrawal) {
        sendCommandToAccount(withdrawal.accountIdentifier(), withdrawal);
    }

    private void getAccount(AccountWriteSide.CommandGetAccount commandGetAccount) {
        sendCommandToAccount(commandGetAccount.accountIdentifier(), commandGetAccount);
    }

    private void sendCommandToAccount(AccountIdentifier accountIdentifier, Object message) {
        Option<ActorRef> accountRefOption = context().child(accountIdentifier.identifier());
        if (accountRefOption.isDefined()) {
            accountRefOption.get().forward(message, context());
        }
        else {
            ActorRef accountRef = context().actorOf(AccountWriteSide.props(accountIdentifier), accountIdentifier.identifier());
            accountRef.forward(message, context());
        }
    }
}
