package com.septima.collections;

@FunctionalInterface
public interface ChangeListener<K, T> {

    void accept(K aKey, T oldValue, T newValue);
}
