package com.septima.client;

/**
 *
 * @author mg
 */
public class ClientConstants {
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
    /*
    // Java properties names
    public static final String USER_DIR_PROP_NAME = "user.dir";
    public static final String USER_HOME_PROP_NAME = "user.home";
    public static final String LINE_SEPARATOR_PROP_NAME = "line.separator";
    //public static final String TEMP_DIR_PROP_NAME = "java.io.tmpdir";
    public static final String USER_HOME_SEPTIMA_DIRECTORY_NAME = ".septima";
    */
    // Metadata elements types
    public static final int DB_MD_TYPE_TABLES = 1;
    public static final int DB_MD_TYPE_SCHEMAS = 2;
    // Database dialects are used as keys to select the SQL driver or JDBC driver.
    public static final String SERVER_PROPERTY_ORACLE_DIALECT = "Oracle";
    public static final String SERVER_PROPERTY_MSSQL_DIALECT = "MsSql";
    public static final String SERVER_PROPERTY_POSTGRE_DIALECT = "PostgreSql";
    public static final String SERVER_PROPERTY_MYSQL_DIALECT = "MySql";
    public static final String SERVER_PROPERTY_DB2_DIALECT = "Db2";
    public static final String SERVER_PROPERTY_H2_DIALECT = "H2";
}
