package com.septima.model;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mg
 */
public class Id {

    private static final int COUNTER_DIGITS = 100;
    private static final AtomicLong SEQUENCE = new AtomicLong(System.currentTimeMillis() * COUNTER_DIGITS);

    private static final int EXTENDED_COUNTER_DIGITS = 1000000;
    private static final AtomicLong EXTENDED_SEQUENCE = new AtomicLong(System.currentTimeMillis() * EXTENDED_COUNTER_DIGITS);

    public static long next() {
        return generate(SEQUENCE, COUNTER_DIGITS);
    }

    public static long nextExtended() {
        return generate(EXTENDED_SEQUENCE, EXTENDED_COUNTER_DIGITS);
    }

    private static long generate(AtomicLong aLastId, int aCounterDigits) {
        long newId;
        long id;
        do {
            id = aLastId.get();
            long idMillis = id / aCounterDigits;// Note. Truncation of fractional part is here.
            if (idMillis == System.currentTimeMillis()) {
                long oldCounter = id - idMillis * aCounterDigits;
                long newCounter = oldCounter + 1;
                if (newCounter == aCounterDigits) {
                    // Spin with maximum duration of one millisecond ...
                    long newMillis;
                    do {
                        newMillis = System.currentTimeMillis();
                    } while (newMillis == idMillis);
                    newId = newMillis * aCounterDigits;
                } else {
                    newId = idMillis * aCounterDigits + newCounter;
                }
            } else {
                newId = System.currentTimeMillis() * aCounterDigits;
            }
        } while (!aLastId.compareAndSet(id, newId));
        return newId;
    }
}
