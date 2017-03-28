package akka.sample.persistence;

import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SaveSnapshotSuccess;
import akka.persistence.SnapshotOffer;
import akka.persistence.journal.Tagged;
import scala.Option;
import scala.PartialFunction;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * An account persistence actor with a banking account state.
 */
class AccountWriteSide extends AbstractPersistentActor {
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

    AccountWriteSide(AccountIdentifier accountIdentifier) {
        this.account = new Account(accountIdentifier, CurrencyValue.zero());
    }

    @Override
    public String persistenceId() {
        return account.accountIdentifier().identifier();
    }

    static Props props(AccountIdentifier accountIdentifier) {
        return Props.create(AccountWriteSide.class, accountIdentifier);
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveRecover() {
        return ReceiveBuilder
                .match(EventDeposit.class, this::recoverEventDeposit)
                .match(EventWithdrawal.class, this::recoverEventWithdrawal)
                .match(SnapshotOffer.class, this::recoverSnapshot)
                .match(RecoveryCompleted.class, this::recoveryCompleted)
                .build();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveCommand() {
        return ReceiveBuilder
                .match(CommandDeposit.class, this::receiveCommandDeposit)
                .match(CommandWithdrawal.class, this::receiveCommendWithdrawal)
                .match(CommandGetAccount.class, this::getAccount)
                .match(IdleTimeout.class, this::receiveTimeout)
                .match(SnapshotTick.class, this::snapshotPendingChanges)
                .match(SaveSnapshotSuccess.class, this::snapshotSuccess)
                .build();
    }

    @Override
    public void onPersistFailure(Throwable cause, Object event, long seqNr) {
        super.onPersistFailure(cause, event, seqNr);
    }

    @Override
    public void onPersistRejected(Throwable cause, Object event, long seqNr) {
        super.onPersistRejected(cause, event, seqNr);
    }

    @Override
    public void onRecoveryFailure(Throwable cause, Option<Object> event) {
        super.onRecoveryFailure(cause, event);
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

        persist(asTagged(eventDeposit, "account"), this::depositPersisted);
    }

    private void depositPersisted(Tagged tagged) {
        EventDeposit eventDeposit = (EventDeposit) tagged.payload();
        account.deposit(eventDeposit.amount());
        sender().tell(eventDeposit, self());
        resetIdleTimeout();
        persisted = pendingChanges = true;
        log.info("State change {} deposit {}", account, eventDeposit.amount());
    }

    private void receiveCommendWithdrawal(CommandWithdrawal commandWithdrawal) {
        log.info("Command {}", commandWithdrawal);
        EventWithdrawal eventWithdrawal = new EventWithdrawal(account.accountIdentifier(), commandWithdrawal.amount());

        persist(asTagged(eventWithdrawal, "account"), this::withdrawalPersisted);
    }

    private void withdrawalPersisted(Tagged tagged) {
        EventWithdrawal eventWithdrawal = (EventWithdrawal) tagged.payload();
        account.withdrawal(eventWithdrawal.amount());
        sender().tell(eventWithdrawal, self());
        resetIdleTimeout();
        persisted = pendingChanges = true;
        log.info("State change {} withdraw {}", account, eventWithdrawal.amount());
    }

    private void getAccount(CommandGetAccount commandGetAccount) {
        if (persisted) {
            sender().tell(new GetAccountResponse(account), self());
            log.info("Get account {}", account);
        } else {
            sender().tell(new GetAccountNotFound(commandGetAccount.accountIdentifier()), self());
            log.info("Get account {} not found", commandGetAccount.accountIdentifier);
        }
    }

    private void snapshotPendingChanges(SnapshotTick snapshotTick) {
        if (pendingChanges) {
            saveSnapshot(account);
            pendingChanges = false;
            log.info("Snapshot {} {}", account, snapshotTick);
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
        idleTimeout = context().system().scheduler().scheduleOnce(
                timeout,
                self(),
                new IdleTimeout(timeout),
                context().system().dispatcher(),
                self());
    }

    private void scheduleSnapshot() {
        FiniteDuration interval = Duration.create(5, TimeUnit.SECONDS); // TODO make this configurable

        snapshotScheduler = context().system().scheduler().schedule(
                interval,
                interval,
                self(),
                new SnapshotTick(),
                context().system().dispatcher(),
                self());
    }

    private Tagged asTagged(Object event, String... tags) {
        return new Tagged(event, new HashSet<>(Arrays.asList(tags)));
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

    static abstract class EventTag {
        static void a(ActorSystem actorSystem) {
            Cluster cluster = Cluster.get(actorSystem);
            for (Member member : cluster.state().getMembers()) {
                member.upNumber();
            }
        }
    }

    static class EventDeposit implements Serializable {
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

    static class CommandGetAccount implements Serializable {
        private final AccountIdentifier accountIdentifier;

        CommandGetAccount(AccountIdentifier accountIdentifier) {
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

        Account account() {
            return account;
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
