package akka.sample.persistence;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;


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

        accounts.tell(new CommandDeposit(AccountIdentifier.create("100"), CurrencyValue.create(100)), null);
        accounts.tell(new CommandDeposit(AccountIdentifier.create("200"), CurrencyValue.create(50)), null);
        accounts.tell(new CommandDeposit(AccountIdentifier.create("300"), CurrencyValue.create(50)), null);
        accounts.tell(new CommandDeposit(AccountIdentifier.create("100"), CurrencyValue.create(200)), null);
        accounts.tell(new CommandWithdrawal(AccountIdentifier.create("100"), CurrencyValue.create(99.95)), null);
        accounts.tell(new CommandWithdrawal(AccountIdentifier.create("300"), CurrencyValue.create(25)), null);
        accounts.tell(new CommandDeposit(AccountIdentifier.create("100"), CurrencyValue.create(200)), null);
        accounts.tell(new CommandWithdrawal(AccountIdentifier.create("300"), CurrencyValue.create(25)), null);
        accounts.tell(new CommandDeposit(AccountIdentifier.create("200"), CurrencyValue.create(15.55)), null);
        accounts.tell(new CommandWithdrawal(AccountIdentifier.create("200"), CurrencyValue.create(19.99)), null);
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
}
