package akka.sample.persistence;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.math.BigDecimal;

import static akka.sample.persistence.AccountWriteSide.*;


/**
 * An example of event sourcing with Akka persistence.
 *
 * <p>This example contains an actor that simulates the interaction of actors with persistence actors. The {@link
 * Runner} actor is used to send messages to the {@link AccountsWriteSide}. These messages are commands to be processed by
 * the {@link AccountWriteSide}.</p>
 *
 * <p>The {@link #runExamples()} method sends a series of commands to the {@link Runner} actor, which in turn sends
 * these commands to the {@link AccountsWriteSide}. The {@link Runner} actor is also used to receive response messages
 * from the {@link AccountWriteSide} actors.</p>
 *
 * <p>The tests are split by periods of idle time. This is to allow for showing the the {@link AccountWriteSide}
 * actor stops after a period of idle time. The last thing done at the conclusion of the test run is to perform a
 * series of get commands that retrieve the account entity. The account entity balance is then withdrawn. This is done
 * to set the account balance to zero.</p>
 */
public class ExampleEventSourcing {
    private static final Logger log = LoggerFactory.getLogger(ExampleEventSourcing.class);
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
        ActorRef accounts = actorSystem.actorOf(AccountsWriteSide.props(), "accounts");
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

        sleep(Duration.create("10 seconds"));
        runner.tell(get(AccountIdentifier.create("100")), null);
        runner.tell(get(AccountIdentifier.create("200")), null);
        runner.tell(get(AccountIdentifier.create("300")), null);
        runner.tell(get(AccountIdentifier.create("400")), null);
        runner.tell(get(AccountIdentifier.create("500")), null);
    }

    private CommandDeposit deposit(AccountIdentifier identifier, CurrencyValue amount) {
        return new CommandDeposit(identifier, amount);
    }

    private CommandWithdrawal withdrawal(AccountIdentifier identifier, CurrencyValue amount) {
        return new CommandWithdrawal(identifier, amount);
    }

    private CommandGetAccount get(AccountIdentifier accountIdentifier) {
        return new CommandGetAccount(accountIdentifier);
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
        log.info("Start {} examples", ExampleEventSourcing.class.getSimpleName());
        new ExampleEventSourcing();
    }

    private static class Runner extends AbstractLoggingActor {
        private final ActorRef accounts;

        {
            receive(ReceiveBuilder
                    .match(CommandDeposit.class, this::sendDeposit)
                    .match(CommandWithdrawal.class, this::sendWithdrawal)
                    .match(CommandGetAccount.class, this::sendGetAccountRequest)
                    .match(EventDeposit.class, this::depositCompleted)
                    .match(EventWithdrawal.class, this::withdrawalCompleted)
                    .match(GetAccountResponse.class, this::getAccountResponse)
                    .match(GetAccountNotFound.class, this::getAccountNotFound)
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

        private void sendGetAccountRequest(CommandGetAccount commandGetAccount) {
            accounts.tell(commandGetAccount, self());
        }

        private void depositCompleted(EventDeposit deposit) {
            log().info("Completed {}", deposit);
        }

        private void withdrawalCompleted(EventWithdrawal withdrawal) {
            log().info("Completed {}", withdrawal);
        }

        private void getAccountResponse(GetAccountResponse getAccountResponse) {
            log().info("{}", getAccountResponse);
            zeroAccountBalance(getAccountResponse.account());
        }

        private void getAccountNotFound(GetAccountNotFound getAccountNotFound) {
            log().info("{}", getAccountNotFound);
        }

        private void unhandledMessage(Object message) {
            log().warning("Unhandled message {}", message);
        }

        private void zeroAccountBalance(Account account) {
            if (!account.balance().amount().equals(BigDecimal.ZERO)) {
                accounts.tell(new CommandWithdrawal(account.accountIdentifier(), account.balance()), self());
            }
        }

        static Props props(ActorRef account) {
            return Props.create(Runner.class, account);
        }
    }
}
