package com.septima.application.endpoint;

import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.septima.GenericType;
import com.septima.application.AsyncEndPoint;
import com.septima.application.Data;
import com.septima.application.exceptions.NotPublicException;
import com.septima.application.exceptions.ReadsNotAllowedException;
import com.septima.application.exceptions.WritesNotAllowedException;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.metadata.EntityField;
import com.septima.metadata.Parameter;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SqlEntitiesEndPoint extends AsyncEndPoint {

    static void onPublic(Consumer<SqlEntity> action, Answer answer, SqlEntity entity) {
        if (entity.isPublicAccess()) {
            action.accept(entity);
        } else {
            throw new NotPublicException(entity.getName());
        }
    }

    static void onReadsAllowed(Consumer<SqlEntity> action, Answer answer, SqlEntity entity) {
        if (entity.getReadRoles().isEmpty() || entity.getReadRoles().stream().anyMatch(answer.getRequest()::isUserInRole)) {
            action.accept(entity);
        } else {
            throw new ReadsNotAllowedException(entity.getName(), entity.getReadRoles());
        }
    }

    static void onWritesAllowed(Consumer<SqlEntity> action, Answer answer, SqlEntity entity) {
        if (entity.getWriteRoles().isEmpty() || entity.getWriteRoles().stream().anyMatch(answer.getRequest()::isUserInRole)) {
            action.accept(entity);
        } else {
            throw new WritesNotAllowedException(entity.getName(), entity.getWriteRoles());
        }
    }

    static void onCollectionRef(Consumer<String> action, Answer answer) {
        String pathInfo = answer.getRequest().getPathInfo();
        if (pathInfo != null && pathInfo.length() > 1) {
            try {
                action.accept(pathInfo.endsWith("/") ? pathInfo.substring(1, pathInfo.length() - 1) : pathInfo.substring(1));
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        } else {
            answer.erroneous("No collection reference can be found at the location: '" + answer.getRequest().getRequestURL() + "'");
        }
    }

    static Map<String, GenericType> parametersTypes(SqlEntity aEntity) {
        return aEntity.getParameters().values().stream()
                .collect(Collectors.toMap(Parameter::getName, p -> p.getType() != null ? p.getType() : GenericType.STRING));
    }

    static Map<String, GenericType> fieldsTypes(SqlEntity aEntity) {
        return aEntity.getFields().values().stream()
                .collect(Collectors.toMap(EntityField::getName, f -> f.getType() != null ? f.getType() : GenericType.STRING));
    }

    static void reviveDates(Map<String, Object> aSubject, Map<String, GenericType> aTypes) {
        aSubject.keySet()
                .forEach(key -> {
                    if (aTypes.containsKey(key)) {
                        GenericType type = aTypes.get(key);
                        if (type == GenericType.DATE) {
                            Object value = aSubject.get(key);
                            if (value instanceof String) {
                                StdDateFormat format = new StdDateFormat();
                                try {
                                    aSubject.put(key, format.parse((String) value));
                                } catch (ParseException ex) {
                                    throw new IllegalStateException(ex);
                                }
                            } else if (value instanceof Number) {
                                aSubject.put(key, new Date(((Number) value).longValue()));
                            }
                        }
                    }
                });
    }

    protected transient volatile SqlEntities entities;

    @Override
    protected void prepare() throws Exception {
        entities = Data.getInstance().getEntities();
    }
}
