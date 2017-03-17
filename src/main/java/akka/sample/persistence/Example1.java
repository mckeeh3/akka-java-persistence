package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import static akka.sample.persistence.AccountPersistentActor.*;


/**
 * An example of simple Akka persistence.
 */
public class Example1 {
    private static final Logger log = LoggerFactory.getLogger(Example1.class);
    private final ActorSystem actorSystem;

    {
        actorSystem = startActorSystem();
        runExamples();
        shutdownActorSystem();
    }

    private ActorSystem startActorSystem() {
        log.info("Start actor system");
        return ActorSystem.create("example-1");
    }

    private void runExamples() {
        ActorRef accounts = actorSystem.actorOf(AccountsActor.props(), "accounts");
        ActorRef runner = actorSystem.actorOf(Runner.props(accounts), "runner");

        runner.tell(deposit(AccountIdentifier.create("100"), CurrencyValue.create(100)), null);
        runner.tell(deposit(AccountIdentifier.create("200"), CurrencyValue.create(50)), null);
        runner.tell(deposit(AccountIdentifier.create("300"), CurrencyValue.create(50)), null);
        runner.tell(deposit(AccountIdentifier.create("100"), CurrencyValue.create(200)), null);
        runner.tell(withdrawal(AccountIdentifier.create("100"), CurrencyValue.create(99.95)), null);
        runner.tell(withdrawal(AccountIdentifier.create("300"), CurrencyValue.create(25)), null);
        runner.tell(deposit(AccountIdentifier.create("100"), CurrencyValue.create(200)), null);
        runner.tell(withdrawal(AccountIdentifier.create("300"), CurrencyValue.create(25)), null);
        runner.tell(deposit(AccountIdentifier.create("200"), CurrencyValue.create(15.55)), null);
        runner.tell(withdrawal(AccountIdentifier.create("200"), CurrencyValue.create(19.99)), null);

        sleep(Duration.create("5 seconds"));
        runner.tell(deposit(AccountIdentifier.create("100"), CurrencyValue.create(200)), null);

        sleep(Duration.create("10 seconds"));
        runner.tell(deposit(AccountIdentifier.create("300"), CurrencyValue.create(25)), null);
        runner.tell(withdrawal(AccountIdentifier.create("300"), CurrencyValue.create(25)), null);

        sleep(Duration.create("10 seconds"));
        runner.tell(deposit(AccountIdentifier.create("400"), CurrencyValue.create(123.45)), null);
        runner.tell(withdrawal(AccountIdentifier.create("400"), CurrencyValue.create(123.45)), null);
    }

    private CommandDeposit deposit(AccountIdentifier identifier, CurrencyValue amount) {
        return new CommandDeposit(identifier, amount);
    }

    private CommandWithdrawal withdrawal(AccountIdentifier identifier, CurrencyValue amount) {
        return new CommandWithdrawal(identifier, amount);
    }

    private void shutdownActorSystem() {
        sleep(Duration.create("1 minute"));
        log.info("Shutdown actor system");
        actorSystem.terminate();
        System.exit(0);
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static void main(String[] arguments) {
        log.info("Start {} examples", Example1.class.getSimpleName());
        new Example1();
    }

    private static class Runner extends AbstractLoggingActor {
        private final ActorRef accounts;

        {
            receive(ReceiveBuilder
                    .match(CommandDeposit.class, this::sendDeposit)
                    .match(CommandWithdrawal.class, this::sendWithdrawal)
                    .match(EventDeposit.class, this::depositCompleted)
                    .match(EventWithdrawal.class, this::withdrawalCompleted)
                    .matchAny(this::unhandledMessage)
                    .build());
        }

        private Runner(ActorRef accounts) {
            this.accounts = accounts;
        }

        private void sendDeposit(CommandDeposit deposit) {
            accounts.tell(deposit, self());
        }

        private void sendWithdrawal(CommandWithdrawal withdrawal) {
            accounts.tell(withdrawal, self());
        }

        private void depositCompleted(EventDeposit deposit) {
            log().info("Completed {}", deposit);
        }

        private void withdrawalCompleted(EventWithdrawal withdrawal) {
            log().info("Completed {}", withdrawal);
        }

        private void unhandledMessage(Object message) {
            log().warning("Unhandled message {}", message);
        }

        static Props props(ActorRef account) {
            return Props.create(Runner.class, account);
        }
    }
}
