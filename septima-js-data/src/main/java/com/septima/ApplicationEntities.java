package com.septima;

import com.septima.metadata.Field;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.Parameter;
import com.septima.queries.SqlQuery;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ApplicationEntities implements EntitiesHost {

    private final QueriesProxy queries;

    public ApplicationEntities(QueriesProxy aQueries){
        queries = aQueries;
    }

    @Override
    public Parameter resolveParameter(String aEntityName, String aParamName) throws Exception {
        if (aEntityName != null) {
            SqlQuery query = queries.getQuery(aEntityName);
            if (query != null && query.getEntityName() != null) {
                return query.getParameters().get(aParamName);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public Field resolveField(String aEntityName, String aFieldName) throws Exception {
        if (aEntityName != null) {
            SqlQuery query = queries.getQuery(aEntityName);
            Map<String, JdbcColumn> fields;
            if (query != null && query.getEntityName() != null) {
                fields = query.getFields();
            } else {// It seems, that aEntityName is a table name...
                fields = query.getDatabase().getMetadata().getTable(aEntityName);
            }
            if (fields != null) {
                Field resolved = fields.get(aFieldName);
                String resolvedTableName = resolved != null ? resolved.getTableName() : null;
                resolvedTableName = resolvedTableName != null ? resolvedTableName.toLowerCase() : "";
                if (query != null && query.getWritable() != null && !query.getWritable().contains(resolvedTableName)) {
                    return null;
                } else {
                    return resolved;
                }
            } else {
                Logger.getLogger(DataSources.class.getName()).log(Level.WARNING, "Cant find fields for entity id:{0}", aEntityName);
                return null;
            }
        } else {
            return null;
        }
    }
}
