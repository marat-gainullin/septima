package com.septima.application.endpoint;

import com.septima.GenericType;
import com.septima.application.exceptions.EndPointException;
import com.septima.application.exceptions.InvalidRequestException;
import com.septima.application.exceptions.NoCollectionException;
import com.septima.application.exceptions.NoInstanceException;
import com.septima.changes.InstanceAdd;
import com.septima.changes.InstanceChange;
import com.septima.changes.InstanceRemove;
import com.septima.dataflow.EntityActionsBinder;
import com.septima.metadata.EntityField;
import com.septima.metadata.Parameter;
import com.septima.model.Id;
import com.septima.queries.SqlQuery;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class SqlEntitiesDataEndPoint extends SqlEntitiesEndPoint {

    @Override
    public void get(Answer answer) {
        onCollectionRef(collectionRef -> {
            AtomicInteger paramSequence = new AtomicInteger();
            AtomicInteger aliasSequence = new AtomicInteger();
            if (entities.exists(collectionRef)) {
                onPublic(publicEntity -> onReadsAllowed(entity -> {
                    SqlQuery query = entities.loadQuery(entity.getName());
                    query.requestData(query.parseParameters(Answer.scalars(answer.getRequest().getParameterMap())))
                            .thenAccept(answer::withJsonArray)
                            .exceptionally(answer::exceptionally);
                }, answer, publicEntity), answer, entities.loadEntity(collectionRef));
            } else {
                int lastSlashAt = collectionRef.lastIndexOf('/');
                if (lastSlashAt > 0 && lastSlashAt < collectionRef.length() - 1) {
                    String anotherCollectionRef = collectionRef.substring(0, lastSlashAt);
                    String instanceKey = collectionRef.substring(lastSlashAt + 1);
                    try {
                        onPublic(publicEntity -> onReadsAllowed(entity -> {
                            SqlQuery query = entities.loadQuery(entity.getName());
                            EntityField pkField = query.getExpectedFields().values().stream()
                                    .filter(EntityField::isPk)
                                    .findAny()
                                    .orElseThrow(() -> new IllegalStateException("Entity '" + query.getEntityName() + "' has no a key field"));
                            String keyParamName = "p" + paramSequence.incrementAndGet();
                            while (entity.getParameters().containsKey(keyParamName)) {
                                keyParamName = "p" + paramSequence.incrementAndGet();
                            }
                            String outerAliasName = "a" + aliasSequence.incrementAndGet();
                            while (query.getSqlClause().contains(outerAliasName)) {
                                outerAliasName = "a" + aliasSequence.incrementAndGet();
                            }
                            List<Parameter> anotherParameters = new ArrayList<>(query.getParameters());
                            anotherParameters.add(new Parameter(keyParamName, null, pkField.getType()));
                            SqlQuery anotherQuery = new SqlQuery(query.getDatabase(),
                                    query.getEntityName(),
                                    "Select * from (" + query.getSqlClause() + ") " + outerAliasName + " where " + outerAliasName + "." + pkField.getName() + " = ?",
                                    anotherParameters,
                                    false,
                                    query.getPageSize(),
                                    query.getExpectedFields());
                            Map<String, Object> anotherParametersValues = anotherQuery.parseParameters(Answer.scalars(answer.getRequest().getParameterMap()));
                            Object keyParamValue = GenericType.parseValue(instanceKey, pkField.getType());
                            anotherParametersValues.put(keyParamName, keyParamValue);
                            anotherQuery.requestData(anotherParametersValues)
                                    .thenApply(data -> {
                                        if (data.size() == 1)
                                            return data.get(0);
                                        else if (data.size() > 1)
                                            throw new IllegalStateException("Collection '" + entity.getName() + "' has more than one instance with key: " + pkField.getName() + " = " + keyParamValue);
                                        else
                                            throw new NoInstanceException(entity.getName(), pkField.getName(), "" + keyParamValue);
                                    })
                                    .thenAccept(answer::withJsonObject)
                                    .exceptionally(answer::exceptionally);
                        }, answer, publicEntity), answer, entities.loadEntity(anotherCollectionRef));
                    } catch (UncheckedIOException aex) {
                        if (aex.getCause() instanceof FileNotFoundException) {
                            throw new NoCollectionException(collectionRef + " or " + anotherCollectionRef);
                        } else {
                            throw aex;
                        }
                    }
                } else {
                    throw new NoCollectionException(collectionRef);
                }
            }
        }, answer);
    }

    @Override
    public void post(Answer answer) {
        onCollectionRef(collectionRef -> {
            if (answer.getRequest().getContentType() != null && answer.getRequest().getContentType().toLowerCase().contains("json")) {
                try {
                    onPublic(publicEntity -> onWritesAllowed(entity -> {
                        EntityActionsBinder binder = new EntityActionsBinder(entity);
                        answer.onJsonObject()
                                .thenApply(arrived -> {
                                    EntityField pkField = entity.getFields().values().stream()
                                            .filter(EntityField::isPk)
                                            .findAny()
                                            .orElseThrow(() -> new IllegalStateException("Entity '" + entity.getName() + "' has no a key field"));
                                    Object key = arrived.get(pkField.getName());
                                    if (key == null) {
                                        if (GenericType.LONG == pkField.getType()) {
                                            arrived.put(pkField.getName(), Id.nextExtended());
                                        } else if (GenericType.DOUBLE == pkField.getType()) {
                                            arrived.put(pkField.getName(), Id.next());
                                        } else if (GenericType.STRING == pkField.getType()) {
                                            arrived.put(pkField.getName(), "" + Id.nextExtended());
                                        } else if (GenericType.DATE == pkField.getType()) {
                                            arrived.put(pkField.getName(), Id.nextExtended());
                                        } else {
                                            throw new EndPointException("Can't generate a key automatically for the type '" + pkField.getType().toString() + "' for instance of collection '" + entity.getName() + "'");
                                        }
                                    }
                                    reviveDates(arrived, fieldsTypes(entity));
                                    InstanceAdd action = new InstanceAdd(entity.getName(), arrived);
                                    action.accept(binder);
                                    return entity.getDatabase().commit(binder.getLogEntries())
                                            .thenApply(affected -> arrived.get(pkField.getName()));
                                })
                                .thenCompose(Function.identity())
                                .thenAccept(key -> answer.created("" + key))
                                .exceptionally(answer::exceptionally);
                    }, answer, publicEntity), answer, entities.loadEntity(collectionRef));
                } catch (UncheckedIOException ex) {
                    if (ex.getCause() instanceof FileNotFoundException) {
                        throw new NoCollectionException(collectionRef);
                    } else {
                        throw ex;
                    }
                }
            } else {
                answer.erroneous("Instance creation requires a json body.");
            }
        }, answer);
    }

    @Override
    public void put(Answer answer) {
        onCollectionRef(instanceRef -> {
            if (answer.getRequest().getContentType().toLowerCase().contains("json")) {
                int lastSlashAt = instanceRef.lastIndexOf('/');
                if (lastSlashAt > 0 && lastSlashAt < instanceRef.length() - 1) {
                    String collectionRef = instanceRef.substring(0, lastSlashAt);
                    String instanceKey = instanceRef.substring(lastSlashAt + 1);
                    try {
                        onPublic(publicEntity -> onWritesAllowed(entity -> {
                            EntityField pkField = entity.getFields().values().stream()
                                    .filter(EntityField::isPk)
                                    .findAny()
                                    .orElseThrow(() -> new IllegalStateException("Entity '" + entity.getName() + "' has no a key field"));
                            EntityActionsBinder binder = new EntityActionsBinder(entity);
                            answer.onJsonObject()
                                    .thenApply(arrived -> {
                                        try {
                                            Object parsedKey = GenericType.parseValue(instanceKey, pkField.getType());
                                            reviveDates(arrived, fieldsTypes(entity));
                                            InstanceChange action = new InstanceChange(entity.getName(), Map.of(pkField.getName(), parsedKey), arrived);
                                            action.accept(binder);
                                            return entity.getDatabase().commit(binder.getLogEntries());
                                        } catch (IllegalStateException ex) {
                                            if (ex.getCause() instanceof ParseException) {
                                                throw new NoInstanceException(entity.getName(), pkField.getName(), instanceKey);
                                            } else {
                                                throw ex;
                                            }
                                        } catch (NumberFormatException ex) {
                                            throw new NoInstanceException(entity.getName(), pkField.getName(), instanceKey);
                                        }
                                    })
                                    .thenCompose(Function.identity())
                                    .thenAccept(updated -> {
                                        if (updated > 0) {
                                            answer.ok();
                                        } else {
                                            throw new NoInstanceException(entity.getName(), pkField.getName(), instanceKey);
                                        }
                                    })
                                    .exceptionally(answer::exceptionally);
                        }, answer, publicEntity), answer, entities.loadEntity(collectionRef));
                    } catch (UncheckedIOException aex) {
                        if (aex.getCause() instanceof FileNotFoundException) {
                            throw new NoCollectionException(collectionRef);
                        } else {
                            throw aex;
                        }
                    }
                } else {
                    throw new InvalidRequestException("Can't update whole collection: '" + instanceRef + "'. Update of a whole collection is not supported");
                }
            } else {
                answer.erroneous("Instance creation requires a json body.");
            }
        }, answer);
    }

    @Override
    public void delete(Answer answer) {
        onCollectionRef(instanceRef -> {
            int lastSlashAt = instanceRef.lastIndexOf('/');
            if (lastSlashAt > 0 && lastSlashAt < instanceRef.length() - 1) {
                String collectionRef = instanceRef.substring(0, lastSlashAt);
                String instanceKey = instanceRef.substring(lastSlashAt + 1);
                try {
                    onPublic(publicEntity -> onWritesAllowed(entity -> {
                        EntityActionsBinder binder = new EntityActionsBinder(entity);
                        EntityField pkField = entity.getFields().values().stream()
                                .filter(EntityField::isPk)
                                .findAny()
                                .orElseThrow(() -> new IllegalStateException("Entity '" + entity.getName() + "' has no a key field"));
                        try {
                            Object parsedKey = GenericType.parseValue(instanceKey, pkField.getType());
                            InstanceRemove action = new InstanceRemove(entity.getName(), Map.of(pkField.getName(), parsedKey));
                            action.accept(binder);
                            entity.getDatabase().commit(binder.getLogEntries())
                                    .thenAccept(updated -> {
                                        if (updated > 0) {
                                            answer.ok();
                                        } else {
                                            throw new NoInstanceException(entity.getName(), pkField.getName(), instanceKey);
                                        }
                                    })
                                    .exceptionally(answer::exceptionally);
                        } catch (IllegalStateException ex) {
                            if (ex.getCause() instanceof ParseException) {
                                throw new NoInstanceException(entity.getName(), pkField.getName(), instanceKey);
                            } else {
                                throw ex;
                            }
                        } catch (NumberFormatException ex) {
                            throw new NoInstanceException(entity.getName(), pkField.getName(), instanceKey);
                        }
                    }, answer, publicEntity), answer, entities.loadEntity(collectionRef));
                } catch (UncheckedIOException aex) {
                    if (aex.getCause() instanceof FileNotFoundException) {
                        throw new NoCollectionException(collectionRef);
                    } else {
                        throw aex;
                    }
                }
            } else {
                throw new InvalidRequestException("Can't delete whole collection: '" + instanceRef + "'. Delete of a whole collection is not supported");
            }
        }, answer);
    }
}
