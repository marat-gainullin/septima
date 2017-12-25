package com.septima.dataflow;

import com.septima.metadata.Field;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Reader for jdbc result sets sources. Performs reading of a whole result set and
 * particular subset of rows for the result set. Reading utilizes converters transform
 * produce application-specific data while reading.
 *
 * @author mg
 */
public class ResultSetReader {

    private final Map<String, Field> expectedFields;
    private final StatementResultSetHandler statementResultSetHandler;

    public ResultSetReader(Map<String, Field> aExpectedFields, StatementResultSetHandler aStatementResultSetHandler) {
        statementResultSetHandler = aStatementResultSetHandler;
        expectedFields = aExpectedFields;
    }

    public Collection<Map<String, Object>> readRowSet(ResultSet aResultSet, int aPageSize) throws SQLException {
        Objects.requireNonNull(aResultSet, "aResultSet is required argument");
        ResultSetMetaData lowLevelJdbcFields = aResultSet.getMetaData();
        List<Field> readFields = readFields(lowLevelJdbcFields);
        return readRows(expectedFields != null && !expectedFields.isEmpty() ? expectedFields : readFields.stream().collect(Collectors.toMap(Field::getName, Function.identity())), readFields, aResultSet, aPageSize, aResultSet.getStatement().getConnection());
    }

    private List<Field> readFields(ResultSetMetaData jdbcFields) throws SQLException {
        List<Field> appFields = new ArrayList<>();
        for (int i = 1; i <= jdbcFields.getColumnCount(); i++) {
            // String schemaName = jdbcFields.getSchemaName(i);
            String alias = jdbcFields.getColumnLabel(i);// Column label in jdbc is the name of platypus property
            String columnName = jdbcFields.getColumnName(i);

            appFields.add(new Field(
                    alias != null && !alias.isEmpty() ? alias : columnName,
                    null,
                    columnName,
                    jdbcFields.getTableName(i),
                    statementResultSetHandler.getSqlDriver().getTypesResolver().toApplicationType(jdbcFields.getColumnType(i), jdbcFields.getColumnTypeName(i)),
                    jdbcFields.isNullable(i) == ResultSetMetaData.columnNullable,
                    false,
                    null
            ));
        }
        return appFields;
    }

    private Collection<Map<String, Object>> readRows(Map<String, Field> aExpectedFields, List<Field> aReadFields, ResultSet aResultSet, int aPageSize, Connection aConnection) throws SQLException {
        Collection<Map<String, Object>> oRows = new ArrayList<>();
        while ((aPageSize <= 0 || oRows.size() < aPageSize) && aResultSet.next()) {
            Map<String, Object> jsRow = readRow(aExpectedFields, aReadFields, aResultSet, aConnection);
            oRows.add(jsRow);
        }
        return oRows;
    }

    private Map<String, Object> readRow(Map<String, Field> aExpectedFields, List<Field> aReadFields, ResultSet aResultSet, Connection aConnection) throws SQLException {
        if (aResultSet != null) {
            assert aExpectedFields != null;
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < aReadFields.size(); i++) {
                Field readField = aReadFields.get(i);
                Field expectedField = aExpectedFields.get(readField.getName());
                Field field = expectedField != null ? expectedField : readField;
                Object value = statementResultSetHandler.readTypedValue(aResultSet, i + 1);
                row.put(field.getName(), value);
            }
            return row;
        }
        return null;
    }
}
