package com.septima.queries;

import java.util.*;
import java.util.stream.Collectors;

public class CaseInsensitiveSet extends AbstractSet<String> {

    private static String keyToLowerCase(String aKey) {
        return aKey != null ? aKey.toLowerCase() : null;
    }

    private Set<String> delegate;

    public CaseInsensitiveSet(Set<String> aDelegate) {
        super();
        Objects.requireNonNull(aDelegate, "aDelegate is required argument");
        delegate = aDelegate;
    }

    @Override
    public Iterator<String> iterator() {
        return delegate.iterator();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean add(String s) {
        return delegate.add(keyToLowerCase(s));
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        return delegate.addAll(c.stream()
                .map(CaseInsensitiveSet::keyToLowerCase)
                .collect(Collectors.toSet()));
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(keyToLowerCase((String) o));
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c.stream()
                .map(o -> keyToLowerCase((String) o))
                .collect(Collectors.toSet()));
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(keyToLowerCase((String) o));
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return c.stream()
                .allMatch(e -> delegate.contains(keyToLowerCase((String) e)));
    }

}
