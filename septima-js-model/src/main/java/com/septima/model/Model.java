package com.septima.model;

import com.septima.changes.EntityAction;
import com.septima.changes.EntityCommand;
import com.septima.changes.InstanceAdd;
import com.septima.changes.InstanceChange;
import com.septima.changes.InstanceRemove;
import com.septima.collections.ObservableMap;
import com.septima.entities.SqlEntities;
import com.septima.queries.SqlQuery;

import java.beans.PropertyChangeListener;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Model {

    private final SqlEntities sqlEntities;
    private final List<EntityAction> changes = new ArrayList<>();
    private InstanceAdd lastAdd;
    private Object lastAddSource;
    private InstanceChange lastChange;
    private Object lastChangeSource;

    public Model(SqlEntities aEntities) {
        sqlEntities = aEntities;
    }

    protected static <K, D> void toGroups(D instance, Map<K, Collection<D>> groups, K key) {
        groups.computeIfAbsent(key, k -> new HashSet<>()).add(instance);
    }

    protected static <K, D> void fromGroups(D instance, Map<K, Collection<D>> groups, K key) {
        groups.computeIfAbsent(key, k -> new HashSet<>()).remove(instance);
    }

    /**
     * Returns a {@link Map.Entry} with specified key and value.
     * Unlike {@link Map#entry(Object, Object)} retains {@code null} values to preserve datum structure.
     *
     * @param aKey   A key.
     * @param aValue A value.
     * @param <K>    Map's key type.
     * @param <V>    Map's value type.
     * @return A {@link Map.Entry} with specified key and value.
     */
    public static <K, V> Map.Entry<K, V> entry(K aKey, V aValue) {
        return new AbstractMap.SimpleEntry<>(aKey, aValue);
    }

    /**
     * Returns map with specified entries.
     * Unlike {@link Map#ofEntries(Map.Entry[])} retains {@code null} values to preserve datum structure.
     *
     * @param aEntries An array of entries.
     * @param <K>      Map's key type.
     * @param <V>      Map's value type.
     * @return A map with specified entries.
     * Unlike {@link Map#ofEntries(Map.Entry[])} retains {@code null} values to preserve datum structure.
     */
    public static <K, V> Map<K, V> map(Map.Entry<K, V>... aEntries) {
        Map<K, V> map = new HashMap<>();
        for (Map.Entry<K, V> e : aEntries) {
            Objects.requireNonNull(e.getKey());
            map.put(e.getKey(), e.getValue());
        }
        return Collections.unmodifiableMap(map);
    }

    private <D, K> PropertyChangeListener listener(SqlQuery query, String keyName, Function<D, K> keyMapper) {
        return e -> {
            if (lastAdd != null && lastAddSource == e.getSource()) {
                lastChange = null;
                lastChangeSource = null;
                lastAdd.getData().put(e.getPropertyName(), e.getNewValue());
            } else {
                lastAdd = null;
                lastAddSource = null;
                if (lastChange != null && lastChangeSource == e.getSource()) {
                    lastChange.getData().put(e.getPropertyName(), e.getNewValue());
                } else {
                    Map<String, Object> keys = Map.of(keyName, keyName.equals(e.getPropertyName()) ? e.getOldValue() : keyMapper.apply((D) e.getSource()));
                    Map<String, Object> data = new HashMap<>();
                    data.put(e.getPropertyName(), e.getNewValue());

                    lastChange = new InstanceChange(
                            query.getEntityName(),
                            keys,
                            data);
                    lastChangeSource = e.getSource();
                    changes.add(lastChange);
                }
            }
        };
    }

    public void addEntityCommand(String anEntity, Map<String, Object> parameters) {
        changes.add(new EntityCommand(anEntity, parameters));
    }

    public void dropChanges() {
        changes.clear();
        lastAdd = null;
        lastAddSource = null;
        lastChange = null;
        lastChangeSource = null;
    }

    public CompletableFuture<Integer> save() {
        List<CompletableFuture<Integer>> futures = sqlEntities.bindChanges(changes).entrySet().stream()
                .map(entry -> entry.getKey().commit(entry.getValue()))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{}))
                .thenApply(v -> {
                    dropChanges();
                    return futures.stream()
                            .map(f -> f.getNow(0))
                            .reduce(Integer::sum).orElse(0);
                });
    }

    public class Entity<K, D extends Observable> {
        private final SqlQuery query;
        private final String keyName;
        private final PropertyChangeListener changesReflector;
        private final Map<K, D> byKey = new HashMap<>();
        private final Function<D, K> keyOf;
        private final Function<Map<String, Object>, D> forwardMapper;
        private final Function<D, Map<String, Object>> reverseMapper;
        private final Consumer<D> classifier;
        private final Consumer<D> unclassifier;

        public Entity(String anEntityName, String aKeyFieldName, Function<D, K> aKeyOf,
                      Function<Map<String, Object>, D> aForwardMapper,
                      Function<D, Map<String, Object>> aReverseMapper) {
            this(anEntityName, aKeyFieldName, aKeyOf, aForwardMapper, aReverseMapper, (instance) -> {
            }, (instance) -> {
            });
        }

        public Entity(String anEntityName, String aKeyName, Function<D, K> aKeyOf,
                      Function<Map<String, Object>, D> aForwardMapper,
                      Function<D, Map<String, Object>> aReverseMapper,
                      Consumer<D> aClassifier,
                      Consumer<D> aUnclassifier) {
            query = sqlEntities.loadEntity(anEntityName).toQuery();
            changesReflector = listener(query, aKeyName, aKeyOf);
            keyName = aKeyName;
            keyOf = aKeyOf;
            forwardMapper = aForwardMapper;
            reverseMapper = aReverseMapper;
            classifier = aClassifier;
            unclassifier = aUnclassifier;
        }

        public Function<Map<String, Object>, D> getForwardMapper() {
            return forwardMapper;
        }

        public Function<D, Map<String, Object>> getReverseMapper() {
            return reverseMapper;
        }

        public String getName() {
            return query.getEntityName();
        }

        public Map<K, D> getByKey() {
            return byKey;
        }

        public CompletableFuture<Map<K, D>> query(Map<String, Object> parameters) {
            return query.requestData(parameters)
                    .thenApply(this::toDomain);
        }

        /**
         * Transforms untyped data to map of domain entities.
         * It is used in {@link #query(Map)} method to process results of a query.
         * It also can be used separately with any data. For example, {@code data} can be received from
         * http request/response, or manual {@link SqlQuery} execution. Last case is useful when you want
         * to fetch some data only once with use of a {@code JOIN} and construct multiple domain entities from that data.
         * A classic ORM would do the same thing when it is asked to fetched linked entities with 'EAGER' policy.
         * In both cases you are able to use navigation properties as usual.
         *
         * @param data Collection of #Map instances with properties of domain objects in key-value form.
         * @return Map of domain objects, identified by values of a key property.
         */
        public Map<K, D> toDomain(Collection<Map<String, Object>> data) {
            Function<D, D> onRemoved = removed -> {
                byKey.remove(keyOf.apply(removed));
                unclassifier.accept(removed);
                removed.removeListener(changesReflector);
                return removed;
            };
            Function<D, D> onAdded = added -> {
                byKey.put(keyOf.apply(added), added);
                classifier.accept(added);
                added.addListener(changesReflector);
                return added;
            };
            Map<K, D> domainData = data.stream()
                    .map(datum -> {
                        D instance = forwardMapper.apply(datum);
                        K key = keyOf.apply(instance);
                        if (!byKey.containsKey(key)) {
                            onAdded.apply(instance);
                        }
                        return byKey.get(key);
                    })
                    .collect(Collectors.toMap(keyOf, Function.identity()));
            domainData.values().forEach(classifier);
            return new ObservableMap<>(domainData,
                    (aKey, removedValue, addedValue) -> {
                        lastAdd = null;
                        lastAddSource = null;
                        // WARNING! Don't refactor this as other cases of changes.
                        // The following InstanceChange is not treated as an accumulator because we want to
                        // separate all-fields change from one by one changes.
                        // This is why lastChange is nullified here.
                        lastChange = null;
                        lastChangeSource = null;
                        onRemoved.apply(removedValue);
                        D added = onAdded.apply(addedValue);
                        // Warning! This case is not about instance's fields changes.
                        // It is change of one instance to another.
                        // So, the old instance can be deleted from database and than, another instance should be added with the same key.
                        // But in this case we can just update all fields in the database and avoid extra sql statement.
                        InstanceChange change = new InstanceChange(query.getEntityName(), Map.of(keyName, keyOf.apply(added)), reverseMapper.apply(added));
                        changes.add(change);
                    },
                    (aKey, removedValue, addedValue) -> {
                        lastAdd = null;
                        lastAddSource = null;
                        lastChange = null;
                        lastChangeSource = null;
                        D removed = onRemoved.apply(removedValue);
                        changes.add(new InstanceRemove(query.getEntityName(), Map.of(keyName, keyOf.apply(removed))));
                    },
                    (aKey, removedValue, addedValue) -> {
                        D added = onAdded.apply(addedValue);
                        lastAdd = new InstanceAdd(query.getEntityName(), reverseMapper.apply(added));
                        lastAddSource = added;
                        lastChange = null;
                        lastChangeSource = null;
                        changes.add(lastAdd);
                    }
            );
        }

        public Map<K, D> of(){
            return toDomain(Set.of());
        }
    }
}
