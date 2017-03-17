package akka.sample.persistence;

import java.io.Serializable;

/**
 * An account identifier.
 */
public class AccountIdentifier implements Serializable {
    private final String identifier;

    private AccountIdentifier(String identifier) {
        this.identifier = identifier;
    }

    static AccountIdentifier create(String identifier) {
        return new AccountIdentifier(identifier);
    }

    String identifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AccountIdentifier that = (AccountIdentifier) o;

        return identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", getClass().getSimpleName(), identifier);
    }
}
