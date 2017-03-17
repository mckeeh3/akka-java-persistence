package akka.sample.persistence;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.NumberFormat;

/**
 * A currency value.
 */
class CurrencyValue implements Serializable {
    private final BigDecimal amount;

    private CurrencyValue(BigDecimal amount) {
        this.amount = amount;
    }

    private CurrencyValue(double amount) {
        this.amount = new BigDecimal(amount);
    }

    private CurrencyValue(int amount) {
        this.amount = new BigDecimal(amount);
    }

    private CurrencyValue(String amount) {
        this.amount = new BigDecimal(amount);
    }

    static CurrencyValue create(BigDecimal amount) {
        return new CurrencyValue(amount);
    }

    static CurrencyValue create(double amount) {
        return new CurrencyValue(amount);
    }

    static CurrencyValue create(int amount) {
        return new CurrencyValue(amount);
    }

    static CurrencyValue create(String amount) {
        return new CurrencyValue(amount);
    }

    BigDecimal amount() {
        return amount;
    }

    static CurrencyValue zero() {
        return new CurrencyValue(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CurrencyValue that = (CurrencyValue) o;

        return amount.equals(that.amount);
    }

    @Override
    public int hashCode() {
        return amount.hashCode();
    }

    @Override
    public String toString() {
        return NumberFormat.getCurrencyInstance().format(amount);
    }
}
