package com.septima.dataflow;

import com.septima.ApplicationTypes;
import com.septima.metadata.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reader for jdbc result sets sources. Performs reading of a whole rowset and
 * particular subset of rows for the rowset. Reading utilizes converters to
 * produce application-specific data while reading.
 *
 * @author mg
 */
public class ResultSetReader {

    public interface GeometryReader {

        String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException;
    }

    public interface TypesResolver {

        String toApplicationType(int aJdbcType, String aRDBMSType);
    }

    protected static final String RESULT_SET_MISSING_EXCEPTION_MSG = "aResultSet argument must be non null";

    protected Map<String, Field> expectedFields;
    protected GeometryReader gReader;
    protected TypesResolver resolver;

    /**
     *
     * @param aReader
     * @param aResolver
     */
    protected ResultSetReader(GeometryReader aReader, TypesResolver aResolver) {
        super();
        gReader = aReader;
        resolver = aResolver;
    }

    /**
     *
     * @param aExpectedFields Fields expected to be in read result set
     * @param aReader
     * @param aResolver
     */
    public ResultSetReader(Map<String, Field> aExpectedFields, GeometryReader aReader, TypesResolver aResolver) {
        this(aReader, aResolver);
        expectedFields = aExpectedFields;
    }

    /**
     * Reads data from ResultSet object and creates new data collection.
     *
     * @param aPageSize Page size of reading process. May be less then zero to
     * indicate that whole data should be fetched.
     * @param aResultSet
     * @return New data collection instance.
     * @throws SQLException
     */
    public Collection<Map<String, Object>> readRowset(ResultSet aResultSet, int aPageSize) throws SQLException {
        try {
            if (aResultSet != null) {
                ResultSetMetaData lowLevelJdbcFields = aResultSet.getMetaData();
                List<Field> readFields = readFields(lowLevelJdbcFields);
                return readRows(expectedFields != null && !expectedFields.isEmpty() ? expectedFields : readFields.stream().collect(Collectors.toMap(f -> f.getName(), f -> f)), readFields, aResultSet, aPageSize, aResultSet.getStatement().getConnection());
            } else {
                throw new SQLException(RESULT_SET_MISSING_EXCEPTION_MSG);
            }
        } catch (Exception ex) {
            if (ex instanceof SQLException) {
                throw (SQLException) ex;
            } else {
                throw new SQLException(ex);
            }
        }
    }

    public List<Field> readFields(ResultSetMetaData jdbcFields) throws SQLException {
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

    /**
     * Reads all rows from result set, returning them as an ArrayList
     * collection.
     *
     * @param aExpectedFields
     * @param aReadFields Fields instance to be used as rowset's metadata.
     * @param aResultSet A result set to read from.
     * @param aPageSize Page size of reading process. May be less then zero to
     * indicate that whole data should be fetched.
     * @return Array of rows had been read.
     * @throws SQLException
     */
    protected Collection<Map<String, Object>> readRows(Map<String, Field> aExpectedFields, List<Field> aReadFields, ResultSet aResultSet, int aPageSize, Connection aConnection) throws SQLException {
        try {
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
        } catch (Exception ex) {
            if (ex instanceof SQLException) {
                throw (SQLException) ex;
            } else {
                throw new SQLException(ex);
            }
        }
    }

    /**
     * Reads single row from result set, returning it as a result.
     *
     * @param aExpectedFields
     * @param aReadFields
     * @param aResultSet Result set to read from.
     * @return The row had been read.
     * @throws SQLException
     */
    protected Map<String, Object> readRow(Map<String, Field> aExpectedFields, List<Field> aReadFields, ResultSet aResultSet, Connection aConnection) throws SQLException {
        if (aResultSet != null) {
            assert aExpectedFields != null;
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= aReadFields.size(); i++) {
                Field readField = aReadFields.get(i - 1);
                Field expectedField = aExpectedFields.get(readField.getName());
                Field field = expectedField != null ? expectedField : readField;
                Object appObject;
                if (ApplicationTypes.GEOMETRY_TYPE_NAME.equals(field.getType())) {
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
