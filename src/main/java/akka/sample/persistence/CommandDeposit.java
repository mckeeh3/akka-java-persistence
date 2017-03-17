package akka.sample.persistence;

import java.io.Serializable;

/**
 * A deposit command.
 */
class CommandDeposit implements Serializable {
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
