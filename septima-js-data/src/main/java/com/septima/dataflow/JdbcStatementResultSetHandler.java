package com.septima.dataflow;

import com.septima.DataTypes;
import com.septima.Parameter;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.sqldrivers.NamedJdbcValue;
import com.septima.sqldrivers.SqlDriver;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JdbcStatementResultSetHandler implements StatementResultSetHandler {

    private final boolean procedure;
    private final SqlDriver sqlDriver;

    public JdbcStatementResultSetHandler(SqlDriver aSqlDriver, boolean aProcedure) {
        sqlDriver = aSqlDriver;
        procedure = aProcedure;
    }

    @Override
    public SqlDriver getSqlDriver() {
        return sqlDriver;
    }

    @Override
    public int assignInParameter(Parameter aParameter, PreparedStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        /*
         * Crazy DBMS-es in most cases can't answer the question about parameter's type properly!
         * PostgreSQL, for example starts answer the question after some time (about 4-8 hours).
         * But before it raises SQLException. And after that, starts transform report TIMESTAMP parameters
         * as DATE parameters.
         * This leads transform parameters values shifting while statement.setDate() and  erroneous select results!
         * So, don't use {@code aStatement.getParameterMetaData().getParameterType()} call chain.
         */
        Object paramValue = aParameter.getValue();
        int jdbcType;
        String sqlTypeName;
        if (DataTypes.GEOMETRY_TYPE_NAME.equals(aParameter.getType())) {
            NamedJdbcValue jv = sqlDriver.geometryFromWkt(aParameter.getName(), aParameter.getValue().toString(), aConnection);
            paramValue = jv.getValue();
            jdbcType = jv.getJdbcType();
            sqlTypeName = jv.getSqlTypeName();
        } else {
            if (paramValue != null || aParameter.getType() == null) {
                jdbcType = assumeJdbcType(paramValue);
                sqlTypeName = null;
            } else {
                jdbcType = calcJdbcType(aParameter.getType(), null);
                sqlTypeName = null;
            }
        }
        int assignedJdbcType = toStatement(paramValue, aParameterIndex, aStatement, jdbcType, sqlTypeName);
        checkOutParameter(aParameter, aStatement, aParameterIndex, jdbcType);
        return assignedJdbcType;
    }

    @Override
    public void acceptOutParameter(Parameter aParameter, CallableStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        if (aParameter.getMode() == Parameter.Mode.Out
                || aParameter.getMode() == Parameter.Mode.InOut) {
            try {
                Object outedParamValue = DataTypes.GEOMETRY_TYPE_NAME.equals(aParameter.getType()) ?
                        sqlDriver.geometryToWkt(aStatement, aParameterIndex, aConnection) :
                        readTypedValue(aStatement, aParameterIndex);
                aParameter.setValue(outedParamValue);
            } catch (SQLException | UncheckedSQLException ex) {
                String pType = aParameter.getType();
                if (pType != null) {
                    switch (pType) {
                        case DataTypes.STRING_TYPE_NAME:
                            aParameter.setValue(aStatement.getString(aParameterIndex));
                            break;
                        case DataTypes.NUMBER_TYPE_NAME:
                            aParameter.setValue(aStatement.getDouble(aParameterIndex));
                            break;
                        case DataTypes.DATE_TYPE_NAME:
                            aParameter.setValue(aStatement.getDate(aParameterIndex));
                            break;
                        case DataTypes.BOOLEAN_TYPE_NAME:
                            aParameter.setValue(aStatement.getBoolean(aParameterIndex));
                            break;
                        default:
                            aParameter.setValue(aStatement.getObject(aParameterIndex));
                    }
                } else {
                    aParameter.setValue(aStatement.getObject(aParameterIndex));
                }
            }
        }
    }

    public Object readTypedValue(Wrapper aRs, int aColumnIndex) throws SQLException {
        try {
            int sqlType = aRs instanceof ResultSet ? ((ResultSet) aRs).getMetaData().getColumnType(aColumnIndex) : ((CallableStatement) aRs).getParameterMetaData().getParameterType(aColumnIndex);
            Object value = null;
            switch (sqlType) {
                case Types.JAVA_OBJECT:
                case Types.DATALINK:
                case Types.DISTINCT:
                case Types.NULL:
                case Types.ROWID:
                case Types.REF:
                case Types.SQLXML:
                case Types.ARRAY:
                case Types.STRUCT:
                case Types.OTHER:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getString(aColumnIndex) : ((CallableStatement) aRs).getString(aColumnIndex);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getBytes(aColumnIndex) : ((CallableStatement) aRs).getBytes(aColumnIndex);
                    break;
                case Types.BLOB:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getBlob(aColumnIndex) : ((CallableStatement) aRs).getBlob(aColumnIndex);
                    if (value != null) {
                        try (InputStream is = ((Blob) value).getBinaryStream()) {
                            value = is.readAllBytes();
                        }
                    }
                    break;
                // clobs
                case Types.CLOB:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getClob(aColumnIndex) : ((CallableStatement) aRs).getClob(aColumnIndex);
                    if (value != null) {
                        try (Reader reader = ((Clob) value).getCharacterStream()) {
                            value = readReader(reader, -1);
                        }
                    }
                    break;
                case Types.NCLOB:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getNClob(aColumnIndex) : ((CallableStatement) aRs).getNClob(aColumnIndex);
                    if (value != null) {
                        try (Reader reader = ((NClob) value).getCharacterStream()) {
                            value = readReader(reader, -1);
                        }
                    }
                    break;
                // numbers
                case Types.DECIMAL:
                case Types.NUMERIC:
                    // target type - BigDecimal
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getBigDecimal(aColumnIndex) : ((CallableStatement) aRs).getBigDecimal(aColumnIndex);
                    break;
                case Types.BIGINT:
                    // target type - BigInteger
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getBigDecimal(aColumnIndex) : ((CallableStatement) aRs).getBigDecimal(aColumnIndex);
                    if (value != null) {
                        value = ((BigDecimal) value).toBigInteger();
                    }
                    break;
                case Types.SMALLINT:
                    // target type - Short
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getShort(aColumnIndex) : ((CallableStatement) aRs).getShort(aColumnIndex);
                    break;
                case Types.TINYINT:
                case Types.INTEGER:
                    // target type - Int
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getInt(aColumnIndex) : ((CallableStatement) aRs).getInt(aColumnIndex);
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    // target type - Float
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getFloat(aColumnIndex) : ((CallableStatement) aRs).getFloat(aColumnIndex);
                    break;
                case Types.DOUBLE:
                    // target type - Double
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getDouble(aColumnIndex) : ((CallableStatement) aRs).getDouble(aColumnIndex);
                    break;
                // strings
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.NVARCHAR:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                case NON_JDBC_LONG_STRING:
                case NON_JDBC_MEDIUM_STRING:
                case NON_JDBC_MEMO_STRING:
                case NON_JDBC_SHORT_STRING:
                    // target type - string
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getString(aColumnIndex) : ((CallableStatement) aRs).getString(aColumnIndex);
                    break;
                // booleans
                case Types.BOOLEAN:
                case Types.BIT:
                    // target type - Boolean
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getBoolean(aColumnIndex) : ((CallableStatement) aRs).getBoolean(aColumnIndex);
                    break;
                // dates, times
                case Types.DATE:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getDate(aColumnIndex) : ((CallableStatement) aRs).getDate(aColumnIndex);
                    break;
                case Types.TIMESTAMP:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getTimestamp(aColumnIndex) : ((CallableStatement) aRs).getTimestamp(aColumnIndex);
                    break;
                case Types.TIME:
                    value = aRs instanceof ResultSet ? ((ResultSet) aRs).getTime(aColumnIndex) : ((CallableStatement) aRs).getTime(aColumnIndex);
                    break;
            }
            if (aRs instanceof ResultSet ? ((ResultSet) aRs).wasNull() : ((CallableStatement) aRs).wasNull()) {
                value = null;
            }
            return value;
        } catch (IOException | UncheckedIOException ex) {
            throw new SQLException(ex);
        }
    }

    private void checkOutParameter(Parameter param, PreparedStatement stmt, int aParameterIndex, int jdbcType) throws SQLException {
        if (procedure && (param.getMode() == Parameter.Mode.Out
                || param.getMode() == Parameter.Mode.InOut)) {
            assert stmt instanceof CallableStatement;
            CallableStatement cStmt = (CallableStatement) stmt;
            cStmt.registerOutParameter(aParameterIndex, jdbcType);
        }
    }

    /**
     * Reads string data from an abstract reader up transform the length or up transform the end of the reader.
     *
     * @param aReader Reader transform read from.
     * @param length  Length indices segment transform be read. It length == -1, than reading is performed until the end indices Reader.
     * @return String, containing data read from Reader.
     * @throws IOException If some error occur while communicating with database.
     */
    private static String readReader(Reader aReader, int length) throws IOException {
        char[] buffer = new char[32];
        StringWriter res = new StringWriter();
        int read;
        int written = 0;
        while ((read = aReader.read(buffer)) != -1) {
            if (length < 0 || written + read <= length) {
                res.write(buffer, 0, read);
                written += read;
            } else {
                res.write(buffer, 0, read - (written + read - length));
                written += length - (written + read);
                break;
            }
        }
        res.flush();
        String str = res.toString();
        assert length < 0 || str.length() == length;
        return str;
    }

    // Ms sql non jdbc string types
    private static final int NON_JDBC_LONG_STRING = 258;
    private static final int NON_JDBC_MEDIUM_STRING = 259;
    private static final int NON_JDBC_MEMO_STRING = 260;
    private static final int NON_JDBC_SHORT_STRING = 261;

    private static BigDecimal number2BigDecimal(Number aNumber) {
        if (aNumber instanceof Float || aNumber instanceof Double) {
            return new BigDecimal(aNumber.doubleValue());
        } else if (aNumber instanceof BigInteger) {
            return new BigDecimal((BigInteger) aNumber);
        } else if (aNumber instanceof BigDecimal) {
            return (BigDecimal) aNumber;
        } else {
            return new BigDecimal(aNumber.longValue());
        }
    }

    private static int toStatement(Object aValue, int aParameterIndex, PreparedStatement aStmt, int aParameterJdbcType, String aParameterSqlTypeName) throws SQLException {
        if (aValue != null) {
            switch (aParameterJdbcType) {
                // Some strange types. No one knows how work with them.
                case Types.JAVA_OBJECT:
                case Types.DATALINK:
                case Types.DISTINCT:
                case Types.NULL:
                case Types.ROWID:
                case Types.REF:
                case Types.SQLXML:
                case Types.ARRAY:
                case Types.OTHER:
                    try {
                        aStmt.setObject(aParameterIndex, aValue, aParameterJdbcType);
                    } catch (SQLException | UncheckedSQLException ex) {
                        aStmt.setNull(aParameterIndex, aParameterJdbcType, aParameterSqlTypeName);
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.STRUCT:
                    try {
                        aStmt.setObject(aParameterIndex, aValue, Types.STRUCT);
                    } catch (SQLException | UncheckedSQLException ex) {
                        aStmt.setNull(aParameterIndex, aParameterJdbcType, aParameterSqlTypeName);
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    // target type - byte[]
                    if (aValue instanceof byte[]) {
                        aStmt.setBytes(aParameterIndex, (byte[]) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.BLOB:
                    // target type - java.sql.Blob
                    if (aValue instanceof Blob) {
                        aStmt.setBlob(aParameterIndex, (Blob) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.CLOB:
                    // target type - java.sql.Clob
                    if (aValue instanceof Clob) {
                        aStmt.setClob(aParameterIndex, (Clob) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.NCLOB:
                    // target type - java.sql.NClob
                    if (aValue instanceof NClob) {
                        aStmt.setNClob(aParameterIndex, (NClob) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    // target type - BigDecimal
                    // target type - BigDecimal
                    BigDecimal castedBigDecimal = null;
                    if (aValue instanceof BigDecimal) {
                        castedBigDecimal = (BigDecimal) aValue;
                    } else if (aValue instanceof Number) {
                        castedBigDecimal = number2BigDecimal((Number) aValue);
                    } else if (aValue instanceof String) {
                        castedBigDecimal = new BigDecimal((String) aValue);
                    } else if (aValue instanceof Boolean) {
                        castedBigDecimal = new BigDecimal(((Boolean) aValue) ? 1 : 0);
                    } else if (aValue instanceof java.util.Date) {
                        castedBigDecimal = new BigDecimal(((java.util.Date) aValue).getTime());
                    }
                    if (castedBigDecimal != null) {
                        aStmt.setBigDecimal(aParameterIndex, castedBigDecimal);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.BIGINT:
                    // target type - BigInteger
                    BigInteger castedBigInteger = null;
                    if (aValue instanceof BigInteger) {
                        castedBigInteger = (BigInteger) aValue;
                    } else if (aValue instanceof Number) {
                        castedBigInteger = BigInteger.valueOf(((Number) aValue).longValue());
                    } else if (aValue instanceof String) {
                        castedBigInteger = new BigInteger((String) aValue);
                    } else if (aValue instanceof Boolean) {
                        castedBigInteger = BigInteger.valueOf(((Boolean) aValue) ? 1 : 0);
                    } else if (aValue instanceof java.util.Date) {
                        castedBigInteger = BigInteger.valueOf(((java.util.Date) aValue).getTime());
                    }
                    if (castedBigInteger != null) {
                        aStmt.setBigDecimal(aParameterIndex, new BigDecimal(castedBigInteger));
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.SMALLINT:
                    // target type - Short
                    Short castedShort = null;
                    if (aValue instanceof Short) {
                        castedShort = (Short) aValue; // To avoid unnecessary boxing / allocation
                    } else if (aValue instanceof Number) {
                        castedShort = ((Number) aValue).shortValue();
                    } else if (aValue instanceof String) {
                        castedShort = Double.valueOf((String) aValue).shortValue();
                    } else if (aValue instanceof Boolean) {
                        castedShort = Integer.valueOf(((Boolean) aValue) ? 1 : 0).shortValue();
                    } else if (aValue instanceof java.util.Date) {
                        castedShort = Integer.valueOf((int) ((java.util.Date) aValue).getTime()).shortValue();
                    }
                    if (castedShort != null) {
                        aStmt.setShort(aParameterIndex, castedShort);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.TINYINT:
                case Types.INTEGER:
                    // target type - Integer
                    Integer castedInteger = null;
                    if (aValue instanceof Integer) {
                        castedInteger = (Integer) aValue; // To avoid unnecessary boxing / allocation
                    } else if (aValue instanceof Number) {
                        castedInteger = ((Number) aValue).intValue();
                    } else if (aValue instanceof String) {
                        castedInteger = Double.valueOf((String) aValue).intValue();
                    } else if (aValue instanceof Boolean) {
                        castedInteger = (Boolean) aValue ? 1 : 0;
                    } else if (aValue instanceof java.util.Date) {
                        castedInteger = (int) ((java.util.Date) aValue).getTime();
                    }
                    if (castedInteger != null) {
                        aStmt.setInt(aParameterIndex, castedInteger);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    // target type - Float
                    Float castedFloat = null;
                    if (aValue instanceof Float) {
                        castedFloat = (Float) aValue; // To avoid unnecessary boxing / allocation
                    } else if (aValue instanceof Number) {
                        castedFloat = ((Number) aValue).floatValue();
                    } else if (aValue instanceof String) {
                        castedFloat = Float.valueOf((String) aValue);
                    } else if (aValue instanceof Boolean) {
                        castedFloat = (Boolean) aValue ? 1f : 0f;
                    } else if (aValue instanceof java.util.Date) {
                        castedFloat = (float) ((java.util.Date) aValue).getTime();
                    }
                    if (castedFloat != null) {
                        aStmt.setFloat(aParameterIndex, castedFloat);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.DOUBLE:
                    // target type - Double
                    Double castedDouble = null;
                    if (aValue instanceof Double) {
                        castedDouble = (Double) aValue; // To avoid unnecessary boxing / allocation
                    } else if (aValue instanceof Number) {
                        castedDouble = ((Number) aValue).doubleValue();
                    } else if (aValue instanceof String) {
                        castedDouble = Double.valueOf((String) aValue);
                    } else if (aValue instanceof Boolean) {
                        castedDouble = (Boolean) aValue ? 1d : 0d;
                    } else if (aValue instanceof java.util.Date) {
                        castedDouble = (double) ((java.util.Date) aValue).getTime();
                    }
                    if (castedDouble != null) {
                        aStmt.setDouble(aParameterIndex, castedDouble);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    // target type - string
                    // target type - string
                    String castedString = null;
                    if (aValue instanceof Number) {
                        castedString = aValue.toString();
                    } else if (aValue instanceof String) {
                        castedString = (String) aValue;
                    } else if (aValue instanceof Boolean) {
                        castedString = ((Boolean) aValue) ? aValue.toString() : "";
                    } else if (aValue instanceof java.util.Date) {
                        castedString = String.valueOf(((java.util.Date) aValue).getTime());
                    } else if (aValue instanceof Clob) {
                        castedString = ((Clob) aValue).getSubString(1, (int) ((Clob) aValue).length());
                    }
                    if (castedString != null) {
                        if (aParameterJdbcType == Types.NCHAR || aParameterJdbcType == Types.NVARCHAR || aParameterJdbcType == Types.LONGNVARCHAR) {
                            aStmt.setNString(aParameterIndex, castedString);
                        } else {
                            aStmt.setString(aParameterIndex, castedString);
                        }
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    // target type - Boolean
                    Boolean castedBoolean = null;
                    if (aValue instanceof Number) {
                        castedBoolean = !(((Number) aValue).intValue() == 0);
                    } else if (aValue instanceof String || aValue instanceof Clob) {
                        String s;
                        if (aValue instanceof String) {
                            s = (String) aValue;
                        } else {
                            s = ((Clob) aValue).getSubString(1, (int) ((Clob) aValue).length());
                        }
                        castedBoolean = !s.isEmpty();
                    } else if (aValue instanceof Boolean) {
                        castedBoolean = (Boolean) aValue;
                    } else if (aValue instanceof java.util.Date) {
                        castedBoolean = !aValue.equals(new java.util.Date(0));
                    }
                    if (castedBoolean != null) {
                        aStmt.setBoolean(aParameterIndex, castedBoolean);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
                case Types.DATE:
                case Types.TIMESTAMP:
                case Types.TIME:
                    // target type - date
                    java.util.Date castedDate = null;
                    if (aValue instanceof Number) {
                        castedDate = new java.util.Date(((Number) aValue).longValue());
                    } else if (aValue instanceof String) {
                        castedDate = new java.util.Date(Long.valueOf((String) aValue));
                    } else if (aValue instanceof Boolean) {
                        castedDate = new java.util.Date(((Boolean) aValue) ? 1 : 0);
                    } else if (aValue instanceof java.util.Date) {
                        castedDate = (java.util.Date) aValue;
                    }
                    if (castedDate != null) {
                        if (aParameterJdbcType == Types.DATE) {
                            aStmt.setDate(aParameterIndex, new java.sql.Date(castedDate.getTime()));//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        } else if (aParameterJdbcType == Types.TIMESTAMP) {
                            aStmt.setTimestamp(aParameterIndex, new java.sql.Timestamp(castedDate.getTime()));//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        } else if (aParameterJdbcType == Types.TIME) {
                            aStmt.setTime(aParameterIndex, new java.sql.Time(castedDate.getTime()));//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        } else {
                            assert false;
                        }
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FALLED_TO_NULL_MSG, aValue.getClass().getName());
                    }
                    break;
            }
        } else {
            try {
                if (aParameterJdbcType == Types.TIME
                        || aParameterJdbcType == Types.TIME_WITH_TIMEZONE
                        || aParameterJdbcType == Types.TIMESTAMP
                        || aParameterJdbcType == Types.TIMESTAMP_WITH_TIMEZONE) {// Crazy jdbc drivers of some databases (PostgreSQL for example) ignore such types, while setting nulls
                    aParameterJdbcType = Types.DATE;
                    aParameterSqlTypeName = null;
                }
                if (aParameterSqlTypeName != null && !aParameterSqlTypeName.isEmpty()) {
                    aStmt.setNull(aParameterIndex, aParameterJdbcType, aParameterSqlTypeName);
                } else {
                    aStmt.setNull(aParameterIndex, aParameterJdbcType);
                }
            } catch (SQLException | UncheckedSQLException ex) {
                aStmt.setNull(aParameterIndex, aParameterJdbcType, aParameterSqlTypeName);
            }
        }
        return aParameterJdbcType;
    }

    private static final String FALLED_TO_NULL_MSG = "Some value falled transform null while tranferring transform a database. May be it''s class is unsupported: {0}";

    private static int assumeJdbcType(Object aValue) {
        int jdbcType;
        if (aValue instanceof CharSequence) {
            jdbcType = Types.VARCHAR;
        } else if (aValue instanceof Number) {
            jdbcType = Types.DOUBLE;
        } else if (aValue instanceof java.util.Date) {
            jdbcType = Types.TIMESTAMP;
        } else if (aValue instanceof Boolean) {
            jdbcType = Types.BOOLEAN;
        } else {
            jdbcType = Types.VARCHAR;
        }
        return jdbcType;
    }

    private static int calcJdbcType(String aType, Object aParamValue) {
        if (aType != null) {
            switch (aType) {
                case DataTypes.STRING_TYPE_NAME:
                    return java.sql.Types.VARCHAR;
                case DataTypes.NUMBER_TYPE_NAME:
                    return java.sql.Types.DOUBLE;
                case DataTypes.DATE_TYPE_NAME:
                    return java.sql.Types.TIMESTAMP;
                case DataTypes.BOOLEAN_TYPE_NAME:
                    return java.sql.Types.BOOLEAN;
                default:
                    return assumeJdbcType(aParamValue);
            }
        } else {
            return assumeJdbcType(aParamValue);
        }
    }

}
