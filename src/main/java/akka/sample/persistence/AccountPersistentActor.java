package akka.sample.persistence;

import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SaveSnapshotSuccess;
import akka.persistence.SnapshotOffer;
import scala.PartialFunction;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * An account persistence actor with a banking account state.
 */
class AccountPersistentActor extends AbstractPersistentActor {
    private final LoggingAdapter log = Logging.getLogger(context().system(), this);
    private Account account;
    private Cancellable snapshotScheduler;
    private Cancellable idleTimeout;
    private boolean persisted = false;
    private boolean pendingChanges = false;

    {
        resetIdleTimeout();
        scheduleSnapshot();
    }

    AccountPersistentActor(AccountIdentifier accountIdentifier) {
        this.account = new Account(accountIdentifier, CurrencyValue.zero());
    }

    @Override
    public String persistenceId() {
        return account.accountIdentifier().identifier();
    }

    static Props props(AccountIdentifier accountIdentifier) {
        return Props.create(AccountPersistentActor.class, accountIdentifier);
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveRecover() {
        return ReceiveBuilder
                .match(EventDeposit.class, this::recoverEventDeposit)
                .match(EventWithdrawal.class, this::recoverEventWithdrawal)
                .match(SnapshotOffer.class, this::recoverSnapshot)
                .build();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveCommand() {
        return ReceiveBuilder
                .match(CommandDeposit.class, this::receiveCommandDeposit)
                .match(CommandWithdrawal.class, this::receiveCommendWithdrawal)
                .match(GetAccountRequest.class, this::getAccount)
                .match(IdleTimeout.class, this::receiveTimeout)
                .match(SnapshotTick.class, this::snapshotPendingChanges)
                .match(SaveSnapshotSuccess.class, this::snapshotSuccess)
                .match(RecoveryCompleted.class, this::recoveryCompleted)
                .build();
    }

    private void recoverEventDeposit(EventDeposit eventDeposit) {
        log.info("Recover {}", eventDeposit);
        account.deposit(eventDeposit.amount());
        persisted = true;
    }

    private void recoverEventWithdrawal(EventWithdrawal eventWithdrawal) {
        log.info("Recover {}", eventWithdrawal);
        account.withdrawal(eventWithdrawal.amount());
        persisted = true;
    }

    private void recoverSnapshot(SnapshotOffer snapshotOffer) {
        log.info("Recover {} {}", snapshotOffer, snapshotOffer.snapshot());
        account = (Account) snapshotOffer.snapshot();
        persisted = true;
    }

    private void receiveCommandDeposit(CommandDeposit commandDeposit) {
        log.info("Command {}", commandDeposit);
        EventDeposit eventDeposit = new EventDeposit(account.accountIdentifier(), commandDeposit.amount());

        persist(eventDeposit, event -> {
            account.deposit(event.amount());
            sender().tell(event, self());
            resetIdleTimeout();
            persisted = pendingChanges = true;
            log.info("State change {}", account);
        });
    }

    private void receiveCommendWithdrawal(CommandWithdrawal commandWithdrawal) {
        log.info("Command {}", commandWithdrawal);
        EventWithdrawal eventWithdrawal = new EventWithdrawal(account.accountIdentifier(), commandWithdrawal.amount());

        persist(eventWithdrawal, event -> {
            account.withdrawal(event.amount());
            sender().tell(event, self());
            resetIdleTimeout();
            persisted = pendingChanges = true;
            log.info("State change {}", account);
        });
    }

    private void getAccount(GetAccountRequest getAccountRequest) {
        if (persisted) {
            sender().tell(new GetAccountResponse(account), self());
        } else {
            sender().tell(new GetAccountNotFound(getAccountRequest.accountIdentifier()), self());
        }
        log.info("Get account {} {}persisted", getAccountRequest.accountIdentifier, persisted ? "" : "not ");
    }

    private void snapshotPendingChanges(SnapshotTick snapshotTick) {
        if (pendingChanges) {
            saveSnapshot(account);
            pendingChanges = false;
            log.info("Snapshot {}", account);
        }
    }

    private void snapshotSuccess(SaveSnapshotSuccess saveSnapshotSuccess) {
        log.info("Snapshot success {}", saveSnapshotSuccess.metadata());
    }

    private void recoveryCompleted(RecoveryCompleted recoveryCompleted) {
        log.info("RecoveryCompleted {}", recoveryCompleted);
    }

    @Override
    public void preStart() throws Exception {
        log.info("Start {}", account);
    }

    @Override
    public void postStop() {
        log.info("Stop {}", account);

        if (idleTimeout != null) {
            idleTimeout.cancel();
        }
        if (snapshotScheduler != null) {
            snapshotScheduler.cancel();
        }
    }

    private void receiveTimeout(IdleTimeout idleTimeout) {
        log.info("Idle timeout {}, {} timeout", account, idleTimeout);
        context().stop(self());
    }

    private void resetIdleTimeout() {
        // This is not working - see https://github.com/akka/akka/issues/20738
        // context().setReceiveTimeout(Duration.create(10, TimeUnit.SECONDS)); // TODO make this configurable
        FiniteDuration timeout = Duration.create(10, TimeUnit.SECONDS); // TODO make this configurable

        if (idleTimeout != null) {
            idleTimeout.cancel();
        }
        idleTimeout = context().system().scheduler().schedule(
                timeout,
                timeout,
                self(),
                new IdleTimeout(timeout),
                context().system().dispatcher(),
                self());
    }

    private void scheduleSnapshot() {
        FiniteDuration interval = Duration.create(10, TimeUnit.SECONDS); // TODO make this configurable

        snapshotScheduler = context().system().scheduler().schedule(
                interval,
                interval,
                self(),
                new SnapshotTick(),
                context().system().dispatcher(),
                self());
    }

    private static class IdleTimeout {
        private final Duration timeout;

        private IdleTimeout(Duration timeout) {
            this.timeout = timeout;
        }

        @Override
        public String toString() {
            return timeout.toString();
        }
    }

    private static class SnapshotTick {
    }

    static class CommandDeposit implements Serializable {
        private final AccountIdentifier accountIdentifier;
        private final CurrencyValue amount;

        CommandDeposit(AccountIdentifier accountIdentifier, CurrencyValue amount) {
            this.accountIdentifier = accountIdentifier;
            this.amount = amount;
        }

        AccountIdentifier accountIdentifier() {
            return accountIdentifier;
        }

        CurrencyValue amount() {
            return amount;
        }

        @Override
        public String toString() {
            return String.format("%s[%s, %s]", getClass().getSimpleName(), accountIdentifier, amount);
        }
    }

    static class CommandWithdrawal implements Serializable {
        private final AccountIdentifier accountIdentifier;
        private final CurrencyValue amount;

        CommandWithdrawal(AccountIdentifier accountIdentifier, CurrencyValue amount) {
            this.accountIdentifier = accountIdentifier;
            this.amount = amount;
        }

        AccountIdentifier accountIdentifier() {
            return accountIdentifier;
        }

        CurrencyValue amount() {
            return amount;
        }

        @Override
        public String toString() {
            return String.format("%s[%s, %s]", getClass().getSimpleName(), accountIdentifier, amount);
        }
    }

    static public class EventDeposit implements Serializable {
        private final AccountIdentifier accountIdentifier;
        private final CurrencyValue amount;
        private final LocalDateTime time;

        EventDeposit(AccountIdentifier accountIdentifier, CurrencyValue amount) {
            this(accountIdentifier, amount, LocalDateTime.now());
        }

        EventDeposit(AccountIdentifier accountIdentifier, CurrencyValue amount, LocalDateTime time) {
            this.accountIdentifier = accountIdentifier;
            this.amount = amount;
            this.time = time;
        }

        AccountIdentifier accountIdentifier() {
            return accountIdentifier;
        }

        CurrencyValue amount() {
            return amount;
        }

        LocalDateTime time() {
            return time;
        }

        @Override
        public String toString() {
            return String.format("%s[%s, %s, %s]", getClass().getSimpleName(), time, accountIdentifier, amount);
        }
    }

    static class EventWithdrawal implements Serializable {
        private final AccountIdentifier accountIdentifier;
        private final CurrencyValue amount;
        private final LocalDateTime time;

        EventWithdrawal(AccountIdentifier accountIdentifier, CurrencyValue amount) {
            this(accountIdentifier, amount, LocalDateTime.now());
        }

        EventWithdrawal(AccountIdentifier accountIdentifier, CurrencyValue amount, LocalDateTime time) {
            this.accountIdentifier = accountIdentifier;
            this.amount = amount;
            this.time = time;
        }

        AccountIdentifier accountIdentifier() {
            return accountIdentifier;
        }

        CurrencyValue amount() {
            return amount;
        }

        LocalDateTime time() {
            return time;
        }

        @Override
        public String toString() {
            return String.format("%s[%s, %s, %s]", getClass().getSimpleName(), time, accountIdentifier, amount);
        }
    }

    static class GetAccountRequest implements Serializable {
        private final AccountIdentifier accountIdentifier;

        GetAccountRequest(AccountIdentifier accountIdentifier) {
            this.accountIdentifier = accountIdentifier;
        }

        AccountIdentifier accountIdentifier() {
            return accountIdentifier;
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", getClass().getSimpleName(), accountIdentifier);
        }
    }

    static class GetAccountResponse implements Serializable {
        private final Account account;

        GetAccountResponse(Account account) {
            this.account = account;
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", getClass().getSimpleName(), account);
        }
    }

    static class GetAccountNotFound implements Serializable {
        private final AccountIdentifier accountIdentifier;

        GetAccountNotFound(AccountIdentifier accountIdentifier) {
            this.accountIdentifier = accountIdentifier;
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", getClass().getSimpleName(), accountIdentifier);
        }
    }
}
