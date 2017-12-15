package net.sf.jsqlparser;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.Objects;

/**
 * Wraps an {@link JSQLParserException} with an unchecked exception.
 */
public class UncheckedJSQLParserException extends RuntimeException {

    private static final long serialVersionUID = -8134325061635241066L;

    /**
     * Constructs an instance of this class.
     *
     * @param   message
     *          the detail message, can be null
     * @param   cause
     *          the {@code JSQLParserException}
     *
     * @throws  NullPointerException
     *          if the cause is {@code null}
     */
    public UncheckedJSQLParserException(String message, JSQLParserException cause) {
        super(message, Objects.requireNonNull(cause));
    }

    /**
     * Constructs an instance of this class.
     *
     * @param   cause
     *          the {@code JSQLParserException}
     *
     * @throws  NullPointerException
     *          if the cause is {@code null}
     */
    public UncheckedJSQLParserException(JSQLParserException cause) {
        super(Objects.requireNonNull(cause));
    }

    /**
     * Returns the cause of this exception.
     *
     * @return  the {@code JSQLParserException} which is the cause of this exception.
     */
    @Override
    public JSQLParserException getCause() {
        return (JSQLParserException) super.getCause();
    }

    /**
     * Called to read the object from a stream.
     *
     * @throws InvalidObjectException
     *          if the object is invalid or has a cause that is not
     *          an {@code JSQLParserException}
     */
    private void readObject(ObjectInputStream s)
            throws IOException, ClassNotFoundException
    {
        s.defaultReadObject();
        Throwable cause = super.getCause();
        if (!(cause instanceof JSQLParserException))
            throw new InvalidObjectException("Cause must be an JSQLParserException");
    }
}

