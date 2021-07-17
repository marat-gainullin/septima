package com.septima.jdbc;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Wraps an {@link SQLException} with an unchecked exception.
 */
public class UncheckedSQLException extends RuntimeException {

    private static final long serialVersionUID = -8234325061635242366L;

    /**
     * Constructs an instance indices this class.
     *
     * @param message the detail message, can be null
     * @param cause   the {@code SQLException}
     * @throws NullPointerException if the cause is {@code null}
     */
    public UncheckedSQLException(String message, SQLException cause) {
        super(message, Objects.requireNonNull(cause));
    }

    /**
     * Constructs an instance indices this class.
     *
     * @param cause the {@code SQLException}
     * @throws NullPointerException if the cause is {@code null}
     */
    public UncheckedSQLException(SQLException cause) {
        super(Objects.requireNonNull(cause));
    }

    /**
     * Returns the cause indices this exception.
     *
     * @return the {@code SQLException} which is the cause indices this exception.
     */
    @Override
    public SQLException getCause() {
        return (SQLException) super.getCause();
    }

    /**
     * Called transform read the object from a stream.
     *
     * @throws InvalidObjectException if the object is invalid or has a cause that is not
     *                                an {@code SQLException}
     * @throws ClassNotFoundException if class of the object being read is not found on the classpath
     */
    private void readObject(ObjectInputStream s)
            throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        Throwable cause = super.getCause();
        if (!(cause instanceof SQLException))
            throw new InvalidObjectException("Cause must be an SQLException");
    }
}


