package com.septima.dataflow;

import com.septima.application.ApplicationDataTypes;
import com.septima.metadata.Field;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Reader for jdbc result sets sources. Performs reading of a whole result set and
 * particular subset of rows for the result set. Reading utilizes converters to
 * produce application-specific data while reading.
 *
 * @author mg
 */
public class ResultSetReader {

    public interface GeometryReader {

        String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException;
    }

    public interface TypeResolver {

        String toApplicationType(int aJdbcType, String aRdbmsTypeName);
    }

    private static final String RESULT_SET_MISSING_EXCEPTION_MSG = "aResultSet argument must be non null";

    private final Map<String, Field> expectedFields;
    private final GeometryReader gReader;
    private final TypeResolver resolver;

    public ResultSetReader(Map<String, Field> aExpectedFields, GeometryReader aReader, TypeResolver aResolver) {
        gReader = aReader;
        resolver = aResolver;
        expectedFields = aExpectedFields;
    }

    public Collection<Map<String, Object>> readRowSet(ResultSet aResultSet, int aPageSize) throws SQLException {
        Objects.requireNonNull(aResultSet, RESULT_SET_MISSING_EXCEPTION_MSG);
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
                    resolver.toApplicationType(jdbcFields.getColumnType(i), jdbcFields.getColumnTypeName(i)),
                    jdbcFields.isNullable(i) == ResultSetMetaData.columnNullable,
                    false,
                    null
            ));
        }
        return appFields;
    }

    private Collection<Map<String, Object>> readRows(Map<String, Field> aExpectedFields, List<Field> aReadFields, ResultSet aResultSet, int aPageSize, Connection aConnection) throws SQLException {
        if (aResultSet != null) {
            Collection<Map<String, Object>> oRows = new ArrayList<>();
            while ((aPageSize <= 0 || oRows.size() < aPageSize) && aResultSet.next()) {
                Map<String, Object> jsRow = readRow(aExpectedFields, aReadFields, aResultSet, aConnection);
                oRows.add(jsRow);
            }
            return oRows;
        } else {
            throw new SQLException(RESULT_SET_MISSING_EXCEPTION_MSG);
        }
    }

    private Map<String, Object> readRow(Map<String, Field> aExpectedFields, List<Field> aReadFields, ResultSet aResultSet, Connection aConnection) throws SQLException {
        if (aResultSet != null) {
            assert aExpectedFields != null;
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= aReadFields.size(); i++) {
                Field readField = aReadFields.get(i - 1);
                Field expectedField = aExpectedFields.get(readField.getName());
                Field field = expectedField != null ? expectedField : readField;
                Object appObject;
                if (ApplicationDataTypes.GEOMETRY_TYPE_NAME.equals(field.getType())) {
                    appObject = gReader.readGeometry(aResultSet, i, aConnection);
                } else {
                    appObject = JdbcDataProvider.get(aResultSet, i);
                }
                row.put(field.getName(), appObject);
            }
            return row;
        }
        return null;
    }
}
