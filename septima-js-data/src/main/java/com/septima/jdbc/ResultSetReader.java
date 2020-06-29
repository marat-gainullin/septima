package com.septima.jdbc;

import com.septima.GenericType;
import com.septima.metadata.EntityField;

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

    private final Map<String, EntityField> expectedFields;
    private final JdbcReaderAssigner jdbcReaderAssigner;

    public ResultSetReader(Map<String, EntityField> aExpectedFields, JdbcReaderAssigner aJdbcReaderAssigner) {
        jdbcReaderAssigner = aJdbcReaderAssigner;
        expectedFields = aExpectedFields;
    }

    public List<Map<String, Object>> readRowSet(ResultSet aResultSet, int aPageSize) throws SQLException {
        Objects.requireNonNull(aResultSet, "aResultSet is required argument");
        ResultSetMetaData lowLevelJdbcFields = aResultSet.getMetaData();
        List<EntityField> jdbcFields = readFields(lowLevelJdbcFields);
        List<EntityField> entityFields = mapFields(expectedFields != null && !expectedFields.isEmpty() ? expectedFields : jdbcFields.stream().collect(Collectors.toMap(EntityField::getName, Function.identity())), jdbcFields);
        return readRows(entityFields, aResultSet, aPageSize, aResultSet.getStatement().getConnection());
    }

    private List<EntityField> readFields(ResultSetMetaData jdbcFields) throws SQLException {
        List<EntityField> appEntityFields = new ArrayList<>();
        for (int i = 1; i <= jdbcFields.getColumnCount(); i++) {
            // String schemaName = jdbcFields.getSchema(i);
            String alias = jdbcFields.getColumnLabel(i);// Column label in JDBC is most likely an alias
            String columnName = jdbcFields.getColumnName(i);

            appEntityFields.add(new EntityField(
                    (alias != null && !alias.isEmpty() ? alias : columnName),
                    null,
                    columnName,
                    jdbcFields.getTableName(i),
                    jdbcReaderAssigner.getSqlDriver().getTypesResolver().toGenericType(jdbcFields.getColumnType(i), jdbcFields.getColumnTypeName(i)),
                    jdbcFields.isNullable(i) == ResultSetMetaData.columnNullable,
                    false,
                    null
            ));
        }
        return appEntityFields;
    }

    private List<EntityField> mapFields(Map<String, EntityField> aExpectedFields, List<EntityField> aJdbcFields){
        List<EntityField> fields = new ArrayList<>(aJdbcFields.size());
        for (EntityField jdbcField : aJdbcFields) {
            EntityField expectedEntityField = aExpectedFields.get(jdbcField.getName());
            fields.add(expectedEntityField != null ? expectedEntityField : jdbcField);
        }
        return fields;
    }

    private List<Map<String, Object>> readRows(List<EntityField> aEntityFields, ResultSet aResultSet, int aPageSize, Connection aConnection) throws SQLException {
        List<Map<String, Object>> oRows = new ArrayList<>();
        while ((aPageSize <= 0 || oRows.size() < aPageSize) && aResultSet.next()) {
            Map<String, Object> jsRow = readRow(aEntityFields, aResultSet, aConnection);
            oRows.add(jsRow);
        }
        return oRows;
    }

    private Map<String, Object> readRow(List<EntityField> aEntityFields, ResultSet aResultSet, Connection aConnection) throws SQLException {
        if (aResultSet != null) {
            assert aEntityFields != null;
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < aEntityFields.size(); i++) {
                EntityField entityField = aEntityFields.get(i);
                Object value;
                if (GenericType.GEOMETRY == entityField.getType()) {
                    value = jdbcReaderAssigner.getSqlDriver().geometryToWkt(aResultSet, i + 1, aConnection);
                } else {
                    Object typedValue = jdbcReaderAssigner.readTypedValue(aResultSet, i + 1);
                    value = entityField.getType() != null ? entityField.getType().narrow(typedValue) : typedValue;
                }
                row.put(entityField.getName(), value);
            }
            return row;
        }
        return null;
    }
}
