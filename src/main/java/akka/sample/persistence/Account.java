package akka.sample.persistence;

import java.io.Serializable;

/**
 * A account with an identifier and a balance.
 */
class Account implements Serializable {
    private final AccountIdentifier accountIdentifier;
    private CurrencyValue balance;

    Account(AccountIdentifier accountIdentifier, CurrencyValue balance) {
        this.accountIdentifier = accountIdentifier;
        this.balance = balance;
    }

    AccountIdentifier accountIdentifier() {
        return accountIdentifier;
    }

    CurrencyValue balance() {
        return balance;
    }

    CurrencyValue deposit(CurrencyValue deposit) {
        balance = CurrencyValue.create(balance.amount().add(deposit.amount()));
        return balance;
    }

    CurrencyValue withdrawal(CurrencyValue withdrawal) {
        balance = CurrencyValue.create(balance.amount().subtract(withdrawal.amount()));
        return balance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Account account = (Account) o;

        return accountIdentifier.equals(account.accountIdentifier) && balance.equals(account.balance);
    }

    @Override
    public int hashCode() {
        int result = accountIdentifier.hashCode();
        result = 31 * result + balance.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s[%s, %s]", getClass().getSimpleName(), accountIdentifier, balance);
    }
}
