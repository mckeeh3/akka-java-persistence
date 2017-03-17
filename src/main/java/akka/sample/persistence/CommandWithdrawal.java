package akka.sample.persistence;

import java.io.Serializable;

/**
 * A withdrawal command.
 */
class CommandWithdrawal implements Serializable {
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
