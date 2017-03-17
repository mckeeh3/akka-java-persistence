package akka.sample.persistence;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * An account deposit event.
 */
public class EventDeposit implements Serializable {
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
