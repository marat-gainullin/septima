package com.septima.jdbc;

import com.septima.GenericType;
import com.septima.metadata.Parameter;
import com.septima.sqldrivers.NamedJdbcValue;
import com.septima.sqldrivers.SqlDriver;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JdbcReaderAssigner {

    private final boolean procedure;
    private final SqlDriver sqlDriver;

    public JdbcReaderAssigner(SqlDriver aSqlDriver, boolean aProcedure) {
        sqlDriver = aSqlDriver;
        procedure = aProcedure;
    }

    public SqlDriver getSqlDriver() {
        return sqlDriver;
    }

    public int assignInParameter(Parameter aParameter, PreparedStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        /*
         * Crazy DBMS-es in most cases can't answer the question about parameter's type properly!
         * PostgreSQL, for example starts answer the question after some time (about 4-8 hours).
         * But before it raises SQLException. And after that, reports TIMESTAMP parameters
         * as DATE parameters. It leads to parameters' values shifting while statement.setDate() and
         * erroneous select results!
         * So, don't use {@code aStatement.getParameterMetaData().getParameterType()} call chain.
         */
        Object paramValue = aParameter.getValue();
        int jdbcType;
        String sqlTypeName;
        if (GenericType.GEOMETRY == aParameter.getType()) {
            NamedJdbcValue jv = sqlDriver.geometryFromWkt(aParameter.getName(), aParameter.getValue() != null ? aParameter.getValue().toString() : null, aConnection);
            paramValue = jv.getValue();
            jdbcType = jv.getJdbcType();
            sqlTypeName = jv.getSqlTypeName();
        } else {
            jdbcType = jdbcTypeBySeptimaType(aParameter.getType(), paramValue);
            sqlTypeName = null;
        }
        int assignedJdbcType = toStatement(paramValue, aStatement, aParameterIndex, jdbcType, sqlTypeName);
        checkOutParameter(aParameter, aStatement, aParameterIndex, jdbcType);
        return assignedJdbcType;
    }

    public void acceptOutParameter(Parameter aParameter, CallableStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        if (aParameter.getMode() == Parameter.Mode.Out
                || aParameter.getMode() == Parameter.Mode.InOut) {
            try {
                Object outedParamValue = GenericType.GEOMETRY == aParameter.getType() ?
                        sqlDriver.geometryToWkt(aStatement, aParameterIndex, aConnection) :
                        aParameter.getType() != null ?
                                aParameter.getType().narrow(readTypedValue(aStatement, aParameterIndex)) :
                                readTypedValue(aStatement, aParameterIndex);
                aParameter.setValue(outedParamValue);
            } catch (SQLException | UncheckedSQLException ex) {
                GenericType pType = aParameter.getType();
                if (pType != null) {
                    switch (pType) {
                        case STRING:
                            aParameter.setValue(aStatement.getString(aParameterIndex));
                            break;
                        case DOUBLE:
                            aParameter.setValue(aStatement.getDouble(aParameterIndex));
                            break;
                        case DATE:
                            aParameter.setValue(aStatement.getDate(aParameterIndex));
                            break;
                        case BOOLEAN:
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
            return BigDecimal.valueOf(aNumber.doubleValue());
        } else if (aNumber instanceof BigInteger) {
            return new BigDecimal((BigInteger) aNumber);
        } else if (aNumber instanceof BigDecimal) {
            return (BigDecimal) aNumber;
        } else {
            return new BigDecimal(aNumber.longValue());
        }
    }

    private static int toStatement(Object aValue, PreparedStatement aStatement, int aValuePosition, int aValueJdbcType, String aValueSqlTypeName) throws SQLException {
        if (aValue != null) {
            switch (aValueJdbcType) {
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
                        aStatement.setObject(aValuePosition, aValue, aValueJdbcType);
                    } catch (SQLException | UncheckedSQLException ex) {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType, aValueSqlTypeName);
                    }
                    break;
                case Types.STRUCT:
                    try {
                        aStatement.setObject(aValuePosition, aValue, Types.STRUCT);
                    } catch (SQLException | UncheckedSQLException ex) {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType, aValueSqlTypeName);
                    }
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    // target type - byte[]
                    if (aValue instanceof byte[]) {
                        aStatement.setBytes(aValuePosition, (byte[]) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
                    }
                    break;
                case Types.BLOB:
                    // target type - java.sql.Blob
                    if (aValue instanceof Blob) {
                        aStatement.setBlob(aValuePosition, (Blob) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
                    }
                    break;
                case Types.CLOB:
                    // target type - java.sql.Clob
                    if (aValue instanceof Clob) {
                        aStatement.setClob(aValuePosition, (Clob) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
                    }
                    break;
                case Types.NCLOB:
                    // target type - java.sql.NClob
                    if (aValue instanceof NClob) {
                        aStatement.setNClob(aValuePosition, (NClob) aValue);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
                    }
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
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
                        aStatement.setBigDecimal(aValuePosition, castedBigDecimal);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        aStatement.setBigDecimal(aValuePosition, new BigDecimal(castedBigInteger));
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        aStatement.setShort(aValuePosition, castedShort);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        aStatement.setInt(aValuePosition, castedInteger);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        aStatement.setFloat(aValuePosition, castedFloat);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        aStatement.setDouble(aValuePosition, castedDouble);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        if (aValueJdbcType == Types.NCHAR || aValueJdbcType == Types.NVARCHAR || aValueJdbcType == Types.LONGNVARCHAR) {
                            aStatement.setNString(aValuePosition, castedString);
                        } else {
                            aStatement.setString(aValuePosition, castedString);
                        }
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        aStatement.setBoolean(aValuePosition, castedBoolean);
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, aValueJdbcType);
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
                        castedDate = new java.util.Date(Long.parseLong((String) aValue));
                    } else if (aValue instanceof Boolean) {
                        castedDate = new java.util.Date(((Boolean) aValue) ? 1 : 0);
                    } else if (aValue instanceof java.util.Date) {
                        castedDate = (java.util.Date) aValue;
                    } else if (aValue instanceof java.time.Instant) {
                        castedDate = java.util.Date.from((java.time.Instant) aValue);
                    }
                    if (castedDate != null) {
                        if (aValueJdbcType == Types.DATE) {
                            aStatement.setDate(aValuePosition, new java.sql.Date(castedDate.getTime()));//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        } else if (aValueJdbcType == Types.TIMESTAMP) {
                            aStatement.setTimestamp(aValuePosition, new java.sql.Timestamp(castedDate.getTime()));//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        } else if (aValueJdbcType == Types.TIME) {
                            aStatement.setTime(aValuePosition, new java.sql.Time(castedDate.getTime()));//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        } else {
                            assert false;
                        }
                    } else {
                        Logger.getLogger(JdbcDataProvider.class.getName()).log(Level.WARNING, FAILED_CAST_TO_DATABASE, aValue.getClass().getName());
                        aStatement.setNull(aValuePosition, Types.DATE); // Crazy jdbc drivers of some databases (PostgreSQL for example) ignore other then Types#DATE types, while setting nulls
                    }
                    break;
            }
        } else {
            try {
                if (aValueJdbcType == Types.TIME
                        || aValueJdbcType == Types.TIME_WITH_TIMEZONE
                        || aValueJdbcType == Types.TIMESTAMP
                        || aValueJdbcType == Types.TIMESTAMP_WITH_TIMEZONE) {// Crazy jdbc drivers of some databases (PostgreSQL for example) ignore such types, while setting nulls
                    aValueJdbcType = Types.DATE;
                    aValueSqlTypeName = null;
                }
                if (aValueSqlTypeName != null && !aValueSqlTypeName.isEmpty()) {
                    aStatement.setNull(aValuePosition, aValueJdbcType, aValueSqlTypeName);
                } else {
                    aStatement.setNull(aValuePosition, aValueJdbcType);
                }
            } catch (SQLException ex) {
                aStatement.setNull(aValuePosition, aValueJdbcType, aValueSqlTypeName);
            }
        }
        return aValueJdbcType;
    }

    private static final String FAILED_CAST_TO_DATABASE = "Some value can't be casted to a database type. May be class of the value is unsupported: {0}. About to substitute it with null.";

    private static int jdbcTypeByValue(Object aValue) {
        int jdbcType;
        if (aValue instanceof CharSequence) {
            jdbcType = Types.VARCHAR;
        } else if (aValue instanceof Integer) {
            jdbcType = Types.INTEGER;
        } else if (aValue instanceof Long) {
            jdbcType = Types.BIGINT;
        } else if (aValue instanceof Float) {
            jdbcType = Types.FLOAT;
        } else if (aValue instanceof Number) {
            jdbcType = Types.DOUBLE;
        } else if (aValue instanceof java.util.Date) {
            jdbcType = Types.TIMESTAMP;
        } else if (aValue instanceof Boolean) {
            jdbcType = Types.BOOLEAN;
        } else if (aValue instanceof byte[]) {
            jdbcType = Types.VARBINARY;
        } else {
            jdbcType = Types.VARCHAR;
        }
        return jdbcType;
    }

    private static int jdbcTypeBySeptimaType(GenericType aType, Object aValue) {
        if (aType != null) {
            switch (aType) {
                case STRING:
                    return Types.VARCHAR;
                case LONG:
                    return Types.BIGINT;
                case DOUBLE:
                    return Types.DOUBLE;
                case DATE:
                    return Types.TIMESTAMP;
                case BOOLEAN:
                    return Types.BOOLEAN;
                default:
                    return jdbcTypeByValue(aValue);
            }
        } else {
            return jdbcTypeByValue(aValue);
        }
    }

}
