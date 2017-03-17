package akka.sample.persistence;

import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.SaveSnapshotSuccess;
import akka.persistence.SnapshotOffer;
import scala.PartialFunction;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import java.util.concurrent.TimeUnit;

/**
 * An account persistence actor with a banking account state.
 */
class AccountPersistentActor extends AbstractPersistentActor {
    private final LoggingAdapter log = Logging.getLogger(context().system(), this);
    private Account account;
    private Cancellable snapshotScheduler;
    private boolean pendingChanges = false;

    {
        receive(ReceiveBuilder
                .match(ReceiveTimeout.class, this::receiveTimeout)
                .build());

        resetIdleTimeout();
        scheduleSnapshot();
    }

    private void receiveTimeout(ReceiveTimeout receiveTimeout) {
        log.info("Idle timeout {} {}", account, receiveTimeout);
        context().stop(self());
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
                .match(ReceiveTimeout.class, this::receiveTimeout)
                .match(SnapshotTick.class, this::snapshotPendingChanges)
                .match(SaveSnapshotSuccess.class, this::snapshotSuccess)
                .build();
    }

    private void recoverEventDeposit(EventDeposit eventDeposit) {
        log.info("Recover {}", eventDeposit);
        account.deposit(eventDeposit.amount());
        resetIdleTimeout();
    }

    private void recoverEventWithdrawal(EventWithdrawal eventWithdrawal) {
        log.info("Recover {}", eventWithdrawal);
        account.withdrawal(eventWithdrawal.amount());
        resetIdleTimeout();
    }

    private void recoverSnapshot(SnapshotOffer snapshotOffer) {
        log.info("Recover {} {}", snapshotOffer, snapshotOffer.snapshot());
        account = (Account) snapshotOffer.snapshot();
    }

    private void receiveCommandDeposit(CommandDeposit commandDeposit) {
        log.info("Command {}", commandDeposit);
        EventDeposit eventDeposit = new EventDeposit(account.accountIdentifier(), commandDeposit.amount());

        persist(eventDeposit, event -> {
            account.deposit(event.amount());
            resetIdleTimeout();
            pendingChanges = true;
            log.info("State change {}", account);
        });
    }

    private void receiveCommendWithdrawal(CommandWithdrawal commandWithdrawal) {
        log.info("Command {}", commandWithdrawal);
        EventWithdrawal eventWithdrawal = new EventWithdrawal(account.accountIdentifier(), commandWithdrawal.amount());

        persist(eventWithdrawal, event -> {
            account.withdrawal(event.amount());
            resetIdleTimeout();
            pendingChanges = true;
            log.info("State change {}", account);
        });
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

    @Override
    public void preStart() throws Exception {
        log.info("Start {}", account);
    }

    @Override
    public void postStop() {
        log.info("Stop {}", account);
        if (snapshotScheduler != null) {
            snapshotScheduler.cancel();
        }
    }

    private void resetIdleTimeout() {
        context().setReceiveTimeout(Duration.create(10, TimeUnit.SECONDS)); // TODO make this configurable
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

    private static class SnapshotTick {
    }
}
