package com.septima.application.endpoint;

import com.septima.entities.SqlEntity;

import java.util.Map;

public abstract class SqlEntitiesDataFlowEndPoint extends SqlEntitiesEndPoint {

    protected abstract Map<String, Object> handleInsertData(Answer answer, SqlEntity aEntity, Map<String, Object> aData);

    protected abstract Map<String, Object> handleUpdateKeys(Answer answer, SqlEntity aEntity, Map<String, Object> aKeys);

    protected abstract Map<String, Object> handleUpdateData(Answer answer, SqlEntity aEntity, Map<String, Object> aData);

    protected abstract Map<String, Object> handleDeleteKeys(Answer answer, SqlEntity aEntity, Map<String, Object> aKeys);

    protected abstract Map<String, Object> handleParameters(Answer answer, SqlEntity aEntity, Map<String, Object> aParameters);
}
