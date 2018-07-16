package com.septima.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ObservableMap<K, V> implements Map<K, V> {
    private final Map<K, V> backingMap;
    private ObservableEntrySet entrySet;
    private ObservableKeySet keySet;
    private ObservableValues values;
    private ChangeListener<K, V> onChange;
    private ChangeListener<K, V> onRemoved;
    private ChangeListener<K, V> onAdded;

    public ObservableMap(Map<K, V> map, ChangeListener<K, V> onChange, ChangeListener<K, V> onRemoved, ChangeListener<K, V> onAdded
    ) {
        this.backingMap = map;
        this.onChange = onChange;
        this.onRemoved = onRemoved;
        this.onAdded = onAdded;
    }

    @Override
    public int size() {
        return backingMap.size();
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return backingMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return backingMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return backingMap.get(key);
    }

    @Override
    public V put(K key, V value) {
        V ret;
        if (backingMap.containsKey(key)) {
            ret = backingMap.put(key, value);
            if (ret == null && value != null || ret != null && !ret.equals(value)) {
                onChange.accept(key, ret, value);
            }
        } else {
            ret = backingMap.put(key, value);
            onAdded.accept(key, ret, value);
        }
        return ret;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        if (!backingMap.containsKey(key)) {
            return null;
        }
        V ret = backingMap.remove(key);
        onRemoved.accept((K) key, ret, null);
        return ret;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        for (Iterator<Entry<K, V>> i = backingMap.entrySet().iterator(); i.hasNext(); ) {
            Entry<K, V> e = i.next();
            K key = e.getKey();
            V val = e.getValue();
            i.remove();
            onRemoved.accept(key, val, null);
        }
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new ObservableKeySet();
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (values == null) {
            values = new ObservableValues();
        }
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new ObservableEntrySet();
        }
        return entrySet;
    }

    @Override
    public String toString() {
        return backingMap.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return backingMap.equals(obj);
    }

    @Override
    public int hashCode() {
        return backingMap.hashCode();
    }

    private class ObservableKeySet implements Set<K> {

        @Override
        public int size() {
            return backingMap.size();
        }

        @Override
        public boolean isEmpty() {
            return backingMap.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return backingMap.keySet().contains(o);
        }

        @Override
        public Iterator<K> iterator() {
            return new Iterator<>() {

                private Iterator<Entry<K, V>> entryIt = backingMap.entrySet().iterator();
                private K lastKey;
                private V lastValue;

                @Override
                public boolean hasNext() {
                    return entryIt.hasNext();
                }

                @Override
                public K next() {
                    Entry<K, V> last = entryIt.next();
                    lastKey = last.getKey();
                    lastValue = last.getValue();
                    return last.getKey();
                }

                @Override
                public void remove() {
                    entryIt.remove();
                    onRemoved.accept(lastKey, lastValue, null);
                }

            };
        }

        @Override
        public Object[] toArray() {
            return backingMap.keySet().toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return backingMap.keySet().toArray(a);
        }

        @Override
        public boolean add(K e) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public boolean remove(Object o) {
            return ObservableMap.this.remove(o) != null;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return backingMap.keySet().containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends K> c) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return removeRetain(c, false);
        }

        private boolean removeRetain(Collection<?> c, boolean remove) {
            boolean removed = false;
            for (Iterator<Entry<K, V>> i = backingMap.entrySet().iterator(); i.hasNext(); ) {
                Entry<K, V> e = i.next();
                if (remove == c.contains(e.getKey())) {
                    removed = true;
                    K key = e.getKey();
                    V value = e.getValue();
                    i.remove();
                    onRemoved.accept(key, value, null);
                }
            }
            return removed;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return removeRetain(c, true);
        }

        @Override
        public void clear() {
            ObservableMap.this.clear();
        }

        @Override
        public String toString() {
            return backingMap.keySet().toString();
        }

        @Override
        public boolean equals(Object obj) {
            return backingMap.keySet().equals(obj);
        }

        @Override
        public int hashCode() {
            return backingMap.keySet().hashCode();
        }

    }

    private class ObservableValues implements Collection<V> {

        @Override
        public int size() {
            return backingMap.size();
        }

        @Override
        public boolean isEmpty() {
            return backingMap.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return backingMap.values().contains(o);
        }

        @Override
        public Iterator<V> iterator() {
            return new Iterator<V>() {

                private Iterator<Entry<K, V>> entryIt = backingMap.entrySet().iterator();
                private K lastKey;
                private V lastValue;

                @Override
                public boolean hasNext() {
                    return entryIt.hasNext();
                }

                @Override
                public V next() {
                    Entry<K, V> last = entryIt.next();
                    lastKey = last.getKey();
                    lastValue = last.getValue();
                    return lastValue;
                }

                @Override
                public void remove() {
                    entryIt.remove();
                    onRemoved.accept(lastKey, lastValue, null);
                }

            };
        }

        @Override
        public Object[] toArray() {
            return backingMap.values().toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return backingMap.values().toArray(a);
        }

        @Override
        public boolean add(V e) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public boolean remove(Object o) {
            for (Iterator<V> i = iterator(); i.hasNext(); ) {
                if (i.next().equals(o)) {
                    i.remove();
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return backingMap.values().containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return removeRetain(c, true);
        }

        private boolean removeRetain(Collection<?> c, boolean remove) {
            boolean removed = false;
            for (Iterator<Entry<K, V>> i = backingMap.entrySet().iterator(); i.hasNext(); ) {
                Entry<K, V> e = i.next();
                if (remove == c.contains(e.getValue())) {
                    removed = true;
                    K key = e.getKey();
                    V value = e.getValue();
                    i.remove();
                    onRemoved.accept(key, value, null);
                }
            }
            return removed;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return removeRetain(c, false);
        }

        @Override
        public void clear() {
            ObservableMap.this.clear();
        }

        @Override
        public String toString() {
            return backingMap.values().toString();
        }

        @Override
        public boolean equals(Object obj) {
            return backingMap.values().equals(obj);
        }

        @Override
        public int hashCode() {
            return backingMap.values().hashCode();
        }
    }

    private class ObservableEntry implements Entry<K, V> {

        private final Entry<K, V> backingEntry;

        public ObservableEntry(Entry<K, V> backingEntry) {
            this.backingEntry = backingEntry;
        }

        @Override
        public K getKey() {
            return backingEntry.getKey();
        }

        @Override
        public V getValue() {
            return backingEntry.getValue();
        }

        @Override
        public V setValue(V value) {
            V oldValue = backingEntry.setValue(value);
            onChange.accept(getKey(), oldValue, value);
            return oldValue;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry e = (Map.Entry) o;
            Object k1 = getKey();
            Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public final int hashCode() {
            return (getKey() == null ? 0 : getKey().hashCode())
                    ^ (getValue() == null ? 0 : getValue().hashCode());
        }

        @Override
        public final String toString() {
            return getKey() + "=" + getValue();
        }

    }

    private class ObservableEntrySet implements Set<Entry<K, V>> {

        @Override
        public int size() {
            return backingMap.size();
        }

        @Override
        public boolean isEmpty() {
            return backingMap.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return backingMap.entrySet().contains(o);
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new Iterator<Entry<K, V>>() {

                private Iterator<Entry<K, V>> backingIt = backingMap.entrySet().iterator();
                private K lastKey;
                private V lastValue;

                @Override
                public boolean hasNext() {
                    return backingIt.hasNext();
                }

                @Override
                public Entry<K, V> next() {
                    Entry<K, V> last = backingIt.next();
                    lastKey = last.getKey();
                    lastValue = last.getValue();
                    return new ObservableEntry(last);
                }

                @Override
                public void remove() {
                    backingIt.remove();
                    onRemoved.accept(lastKey, lastValue, null);
                }
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object[] toArray() {
            Object[] array = backingMap.entrySet().toArray();
            for (int i = 0; i < array.length; ++i) {
                array[i] = new ObservableEntry((Entry<K, V>) array[i]);
            }
            return array;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] a) {
            T[] array = backingMap.entrySet().toArray(a);
            for (int i = 0; i < array.length; ++i) {
                array[i] = (T) new ObservableEntry((Entry<K, V>) array[i]);
            }
            return array;
        }

        @Override
        public boolean add(Entry<K, V> e) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean remove(Object o) {
            boolean ret = backingMap.entrySet().remove(o);
            if (ret) {
                Entry<K, V> entry = (Entry<K, V>) o;
                onRemoved.accept(entry.getKey(), entry.getValue(), null);
            }
            return ret;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return backingMap.entrySet().containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Entry<K, V>> c) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return removeRetain(c, false);
        }

        private boolean removeRetain(Collection<?> c, boolean remove) {
            boolean removed = false;
            for (Iterator<Entry<K, V>> i = backingMap.entrySet().iterator(); i.hasNext(); ) {
                Entry<K, V> e = i.next();
                if (remove == c.contains(e)) {
                    removed = true;
                    K key = e.getKey();
                    V value = e.getValue();
                    i.remove();
                    onRemoved.accept(key, value, null);
                }
            }
            return removed;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return removeRetain(c, true);
        }

        @Override
        public void clear() {
            ObservableMap.this.clear();
        }

        @Override
        public String toString() {
            return backingMap.entrySet().toString();
        }

        @Override
        public boolean equals(Object obj) {
            return backingMap.entrySet().equals(obj);
        }

        @Override
        public int hashCode() {
            return backingMap.entrySet().hashCode();
        }

    }
}
