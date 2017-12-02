package com.septima.cache;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Auto updated cache, without shrink.
 *
 * @param <E>
 * @author mg
 */
public abstract class ActualCache<E> {
    
    public static class ActualCacheEntry<E> {

        private final E value;
        private final Date timeStamp;

        public ActualCacheEntry(E aValue, Date aTimeStamp) {
            super();
            value = aValue;
            timeStamp = aTimeStamp;
        }

        public E getValue() {
            return value;
        }

        public Date getTimeStamp() {
            return timeStamp;
        }
    }

    protected final Map<String, ActualCacheEntry<E>> entries = new ConcurrentHashMap<>();

    public E get(String aName, File aFile) throws Exception {
        ActualCacheEntry<E> cached = entries.get(aName);
        Date cachedTime = null;
        if (cached != null) {
            cachedTime = cached.getTimeStamp();
        }
        Date filesModified = new Date(aFile.lastModified());
        if (cachedTime == null || filesModified.after(cachedTime)) {
            E parsed = parse(aName, aFile);
            cached = new ActualCacheEntry<>(parsed, filesModified);
            entries.put(aName, cached);
        }
        return cached != null ? cached.getValue() : null;
    }

    protected abstract E parse(String aName, File aFile) throws Exception;
}
