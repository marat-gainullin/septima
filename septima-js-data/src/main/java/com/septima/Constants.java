package com.septima;

/**
 *
 * @author mg
 */
public class Constants {

    public static final String JDBCFKS_FKTABLE_SCHEM = "FKTABLE_SCHEM";
    public static final String JDBCFKS_FKTABLE_NAME = "FKTABLE_NAME";
    public static final String JDBCFKS_FKCOLUMN_NAME = "FKCOLUMN_NAME";
    public static final String JDBCFKS_FK_NAME = "FK_NAME";
    public static final String JDBCFKS_FKUPDATE_RULE = "UPDATE_RULE";
    public static final String JDBCFKS_FKDELETE_RULE = "DELETE_RULE";
    public static final String JDBCFKS_FKDEFERRABILITY = "DEFERRABILITY";
    public static final String JDBCFKS_FKPKTABLE_SCHEM = "PKTABLE_SCHEM";
    public static final String JDBCFKS_FKPKTABLE_NAME = "PKTABLE_NAME";
    public static final String JDBCFKS_FKPKCOLUMN_NAME = "PKCOLUMN_NAME";
    public static final String JDBCFKS_FKPK_NAME = "PK_NAME";
    public static final String JDBCCOLS_TABLE_SCHEM = "TABLE_SCHEM";
    public static final String JDBCCOLS_TABLE_NAME = "TABLE_NAME";
    public static final String JDBCCOLS_TABLE_DESC = "TABLE_DESCRIPTION";
    public static final String JDBCCOLS_COLUMN_NAME = "COLUMN_NAME";
    public static final String JDBCCOLS_REMARKS = "REMARKS";
    public static final String JDBCCOLS_DATA_TYPE = "DATA_TYPE";
    public static final String JDBCCOLS_TYPE_NAME = "TYPE_NAME";
    public static final String JDBCCOLS_TABLE_TYPE = "TABLE_TYPE";
    public static final String JDBCCOLS_COLUMN_SIZE = "COLUMN_SIZE";
    public static final String JDBCCOLS_DECIMAL_DIGITS = "DECIMAL_DIGITS";
    public static final String JDBCCOLS_NUM_PREC_RADIX = "NUM_PREC_RADIX";
    public static final String JDBCCOLS_NULLABLE = "NULLABLE";
    public static final String JDBCPKS_TABLE_SCHEM = JDBCCOLS_TABLE_SCHEM;
    public static final String JDBCPKS_TABLE_NAME = JDBCCOLS_TABLE_NAME;
    public static final String JDBCPKS_COLUMN_NAME = JDBCCOLS_COLUMN_NAME;
    public static final String JDBCPKS_CONSTRAINT_NAME = "PK_NAME";
    public static final String JDBCIDX_TABLE_SCHEM = JDBCCOLS_TABLE_SCHEM;
    public static final String JDBCIDX_TABLE_NAME = JDBCCOLS_TABLE_NAME;
    public static final String JDBCIDX_COLUMN_NAME = JDBCCOLS_COLUMN_NAME;
    public static final String JDBCIDX_NON_UNIQUE = "NON_UNIQUE";      //boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
    public static final String JDBCIDX_INDEX_QUALIFIER = "INDEX_QUALIFIER"; //String => index catalog (may be null); null when TYPE is tableIndexStatistic
    public static final String JDBCIDX_INDEX_NAME = "INDEX_NAME";      //String => index name; null when TYPE is tableIndexStatistic
    public static final String JDBCIDX_TYPE = "TYPE";            //short => index type:
    public static final String JDBCIDX_PRIMARY_KEY = "IS_PKEY";
    public static final String JDBCIDX_FOREIGN_KEY = "FKEY_NAME";
    //tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions
    //tableIndexClustered - this is a clustered index
    //tableIndexHashed - this is a hashed index
    //tableIndexOther - this is some other style of index
    public static final String JDBCIDX_ORDINAL_POSITION = "ORDINAL_POSITION";//short => column sequence number within index; zero when TYPE is tableIndexStatistic
    public static final String JDBCIDX_ASC_OR_DESC = "ASC_OR_DESC";//String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
    public static final String JDBCPKS_TABLE_CAT_FIELD_NAME = "TABLE_CAT";
    public static final String JDBCPKS_TABLE_TYPE_FIELD_NAME = "TABLE_TYPE";
    public static final String JDBCPKS_TABLE_TYPE_TABLE = "TABLE";
    public static final String JDBCPKS_TABLE_TYPE_VIEW = "VIEW";
    public static final String GEOMETRY_TYPE_NAME = "Geometry";//NOI18N
    public static final String STRING_TYPE_NAME = "String";//NOI18N
    public static final String NUMBER_TYPE_NAME = "Number";//NOI18N
    public static final String DATE_TYPE_NAME = "Date";//NOI18N
    public static final String BOOLEAN_TYPE_NAME = "Boolean";//NOI18N
    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    // Metadata elements types
    public static final int DB_MD_TYPE_TABLES = 1;
    public static final int DB_MD_TYPE_SCHEMAS = 2;
    // Database dialects are used as keys to select the SQL driver or JDBC driver.
    public static final String GENERIC_DIALECT = "Generic";
    public static final String ORACLE_DIALECT = "Oracle";
    public static final String MSSQL_DIALECT = "MsSql";
    public static final String POSTGRE_DIALECT = "PostgreSql";
    public static final String MYSQL_DIALECT = "MySql";
    public static final String DB2_DIALECT = "Db2";
    public static final String H2_DIALECT = "H2";
    //
    private static final String SELECT_BY_FIELD_QUERY = "select * from %s where Upper(%s) = :%s";
    private static final String TYPES_INFO_TRACE_MSG = "Getting types info from data source '%s'";
    private static final String UNKNOWN_DATASOURCE_IN_COMMIT = "Unknown data source: %s. Can't commit to it.";
    private static final String UNSUPPORTED_DATASOURCE_IN_COMMIT = "Unsupported data source: %s. Can't commit to it.";
    public static final String STORED_QUERY_REF_PREFIX = "#";
    public static final String TABLE_NAME_2_SQL = "select * from %s";
    public static final String PARAMETER_NAME_REGEXP = "(?<=[^:]):{1}([A-za-z]\\w*\\b)";
    public static final String PROPERTIES_VALUE_REGEXP = "={1}([A-za-z]\\w+\\b)";
    public static final String SQL_SELECT_COMMON_WHERE_BY_FIELD = "select * from %s where %s.%s = :%s";
    public static final String SQL_SELECT_COMMON_WHERE_ISNULL_FIELD = "select * from %s where %s.%s is null";
    public static final String SQL_PARAMETER_FIELD_VALUE = "fieldValue";
    public static final String SQL_UPDATE_COMMON_WHERE_BY_FIELD = "update %s set %s = %s where %s.%s = :" + SQL_PARAMETER_FIELD_VALUE;
    public static final String SQL_UPDATE2_COMMON_WHERE_BY_FIELD = "update %s set %s = %s, %s = %s where %s.%s = :" + SQL_PARAMETER_FIELD_VALUE;
    public static final String SQL_UPDATE3_COMMON_WHERE_BY_FIELD = "update %s set %s = %s, %s = %s, %s = %s where %s.%s = :" + SQL_PARAMETER_FIELD_VALUE;
    public static final String SQL_UPDATE4_COMMON_WHERE_BY_FIELD = "update %s set %s = %s, %s = %s, %s = %s, %s = %s where %s.%s = :" + SQL_PARAMETER_FIELD_VALUE;
    public static final String SQL_DELETE_COMMON_WHERE_BY_FIELD = "delete from %s where %s.%s = :" + SQL_PARAMETER_FIELD_VALUE;
    public static final String SQL_INSERT_COMMON_ID_FIELD = "insert into %s columns = (%s) values = ( :" + SQL_PARAMETER_FIELD_VALUE + ")";
    public static final String SQL_MAX_COMMON_BY_FIELD = "select max(%s) %s from %s";
}
