package com.septima.queries;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CaseInsensitiveMap<V> extends AbstractMap<String, V> {

    private static final AtomicLong sequence = new AtomicLong();

    private final String nullKey = "null-" + sequence.incrementAndGet();

    private String keyToLowerCase(String aKey) {
        return aKey != null ? aKey.toLowerCase() : nullKey;
    }

    private Map<String, V> delegate;

    public CaseInsensitiveMap(Map<String, V> aDelegate) {
        delegate = aDelegate;
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        return delegate.entrySet();
    }

    @Override
    public V get(Object key) {
        return delegate.get(keyToLowerCase((String) key));
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return delegate.getOrDefault(keyToLowerCase((String) key), defaultValue);
    }

    @Override
    public V put(String key, V value) {
        return delegate.put(keyToLowerCase(key), value);
    }

    @Override
    public V putIfAbsent(String key, V value) {
        return delegate.putIfAbsent(keyToLowerCase(key), value);
    }

    @Override
    public V remove(Object key) {
        return delegate.remove(keyToLowerCase((String) key));
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        m.forEach((k, v) -> delegate.put(keyToLowerCase(k), v));
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(keyToLowerCase((String) key));
    }

    @Override
    public V compute(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        return delegate.compute(keyToLowerCase(key), remappingFunction);
    }

    @Override
    public V computeIfAbsent(String key, Function<? super String, ? extends V> mappingFunction) {
        return delegate.computeIfAbsent(keyToLowerCase(key), mappingFunction);
    }

    @Override
    public V computeIfPresent(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        return delegate.computeIfPresent(keyToLowerCase(key), remappingFunction);
    }

    @Override
    public V merge(String key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return delegate.merge(keyToLowerCase(key), value, remappingFunction);
    }
}
