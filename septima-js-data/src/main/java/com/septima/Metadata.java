package com.septima;

import com.septima.jdbc.ColumnsIndicies;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.*;
import com.septima.dataflow.StatementsGenerator;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.SqlDriver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

/**
 * @author mg
 */
public class Metadata implements StatementsGenerator.TablesContainer {

    private static final String JDBCFKS_FKTABLE_SCHEM = "FKTABLE_SCHEM";
    private static final String JDBCFKS_FKTABLE_NAME = "FKTABLE_NAME";
    private static final String JDBCFKS_FKCOLUMN_NAME = "FKCOLUMN_NAME";
    private static final String JDBCFKS_FK_NAME = "FK_NAME";
    private static final String JDBCFKS_FKUPDATE_RULE = "UPDATE_RULE";
    private static final String JDBCFKS_FKDELETE_RULE = "DELETE_RULE";
    private static final String JDBCFKS_FKDEFERRABILITY = "DEFERRABILITY";
    private static final String JDBCFKS_FKPKTABLE_SCHEM = "PKTABLE_SCHEM";
    private static final String JDBCFKS_FKPKTABLE_NAME = "PKTABLE_NAME";
    private static final String JDBCFKS_FKPKCOLUMN_NAME = "PKCOLUMN_NAME";
    private static final String JDBCFKS_FKPK_NAME = "PK_NAME";
    private static final String JDBCCOLS_TABLE_SCHEM = "TABLE_SCHEM";
    private static final String JDBCCOLS_TABLE_NAME = "TABLE_NAME";
    //private static final String JDBCCOLS_TABLE_DESC = "TABLE_DESCRIPTION";
    private static final String JDBCCOLS_COLUMN_NAME = "COLUMN_NAME";
    private static final String JDBCCOLS_REMARKS = "REMARKS";
    private static final String JDBCCOLS_DATA_TYPE = "DATA_TYPE";
    private static final String JDBCCOLS_TYPE_NAME = "TYPE_NAME";
    //private static final String JDBCCOLS_TABLE_TYPE = "TABLE_TYPE";
    private static final String JDBCCOLS_COLUMN_SIZE = "COLUMN_SIZE";
    private static final String JDBCCOLS_DECIMAL_DIGITS = "DECIMAL_DIGITS";
    private static final String JDBCCOLS_NUM_PREC_RADIX = "NUM_PREC_RADIX";
    private static final String JDBCCOLS_NULLABLE = "NULLABLE";
    private static final String JDBCPKS_TABLE_SCHEM = JDBCCOLS_TABLE_SCHEM;
    private static final String JDBCPKS_TABLE_NAME = JDBCCOLS_TABLE_NAME;
    private static final String JDBCPKS_COLUMN_NAME = JDBCCOLS_COLUMN_NAME;
    private static final String JDBCPKS_CONSTRAINT_NAME = "PK_NAME";
    //private static final String JDBCIDX_TABLE_SCHEM = JDBCCOLS_TABLE_SCHEM;
    //private static final String JDBCIDX_TABLE_NAME = JDBCCOLS_TABLE_NAME;
    private static final String JDBCIDX_COLUMN_NAME = JDBCCOLS_COLUMN_NAME;
    private static final String JDBCIDX_NON_UNIQUE = "NON_UNIQUE";      //boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
    //private static final String JDBCIDX_INDEX_QUALIFIER = "INDEX_QUALIFIER"; //String => index catalog (may be null); null when TYPE is tableIndexStatistic
    private static final String JDBCIDX_INDEX_NAME = "INDEX_NAME";      //String => index name; null when TYPE is tableIndexStatistic
    private static final String JDBCIDX_TYPE = "TYPE";            //short => index type:
    //private static final String JDBCIDX_PRIMARY_KEY = "IS_PKEY";
    //private static final String JDBCIDX_FOREIGN_KEY = "FKEY_NAME";
    //tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions
    //tableIndexClustered - this is a clustered index
    //tableIndexHashed - this is a hashed index
    //tableIndexOther - this is some other style of index
    private static final String JDBCIDX_ORDINAL_POSITION = "ORDINAL_POSITION";//short => column sequence number within index; zero when TYPE is tableIndexStatistic
    private static final String JDBCIDX_ASC_OR_DESC = "ASC_OR_DESC";//String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
    //private static final String JDBCPKS_TABLE_CAT_FIELD_NAME = "TABLE_CAT";
    //private static final String JDBCPKS_TABLE_TYPE_FIELD_NAME = "TABLE_TYPE";
    private static final String JDBCPKS_TABLE_TYPE_TABLE = "TABLE";
    private static final String JDBCPKS_TABLE_TYPE_VIEW = "VIEW";

    private static class CaseInsensitiveSet extends HashSet<String> {

        private String keyToLowerCase(String aKey) {
            return aKey != null ? aKey.toLowerCase() : null;
        }

        @Override
        public boolean add(String s) {
            return super.add(keyToLowerCase(s));
        }

        @Override
        public boolean addAll(Collection<? extends String> c) {
            return c.stream()
                    .map(e -> super.add(keyToLowerCase(e)))
                    .anyMatch(b -> b);
        }

        @Override
        public boolean remove(Object o) {
            return super.remove(keyToLowerCase((String) o));
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return c.stream()
                    .map(e -> super.remove(keyToLowerCase((String) e)))
                    .anyMatch(b -> b);
        }

        @Override
        public boolean contains(Object o) {
            return super.contains(keyToLowerCase((String) o));
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return c.stream()
                    .allMatch(e -> super.contains(keyToLowerCase((String) e)));
        }
    }

    private static class CaseInsensitiveMap<V> extends LinkedHashMap<String, V> {

        private String keyToLowerCase(String aKey) {
            return aKey != null ? aKey.toLowerCase() : null;
        }

        @Override
        public V get(Object key) {
            return super.get(keyToLowerCase((String) key));
        }

        @Override
        public V put(String key, V value) {
            return super.put(keyToLowerCase(key), value);
        }

        @Override
        public V remove(Object key) {
            return super.remove(keyToLowerCase((String) key));
        }

        @Override
        public boolean containsKey(Object key) {
            return super.containsKey(keyToLowerCase((String) key));
        }

    }

    private static class ConcurrentCaseInsensitiveMap<V> extends ConcurrentHashMap<String, V> {

        private static final AtomicLong sequence = new AtomicLong();

        private final String nullKey = "null-" + sequence.incrementAndGet();

        private String keyToLowerCase(String aKey) {
            return aKey != null ? aKey.toLowerCase() : nullKey;
        }

        @Override
        public V get(Object key) {
            return super.get(keyToLowerCase((String) key));
        }

        @Override
        public V put(String key, V value) {
            return super.put(keyToLowerCase(key), value);
        }

        @Override
        public V remove(Object key) {
            return super.remove(keyToLowerCase((String) key));
        }

        @Override
        public boolean containsKey(Object key) {
            return super.containsKey(keyToLowerCase((String) key));
        }

    }

    private final DataSource dataSource;
    private final Set<String> schemas;
    // Schema, Table, Field
    private final Map<String, Map<String, Map<String, JdbcColumn>>> schemasTablesColumns = new ConcurrentCaseInsensitiveMap<>();
    // Schema, Table, Index
    private final Map<String, Map<String, Map<String, TableIndex>>> schemasTablesIndexes = new ConcurrentCaseInsensitiveMap<>();
    private final String defaultSchema;
    private final SqlDriver sqlDriver;

    private Metadata(DataSource aDataSource, Set<String> aSchemas, String aDefaultSchema, SqlDriver aSqlDriver) {
        schemas = aSchemas;
        dataSource = aDataSource;
        defaultSchema = aDefaultSchema;
        sqlDriver = aSqlDriver;
    }

    public static Metadata of(DataSource aDataSource) throws SQLException {
        Set<String> schemas = new CaseInsensitiveSet();
        try (Connection conn = aDataSource.getConnection()) {
            try (ResultSet r = conn.getMetaData().getSchemas()) {
                while (r.next()) {
                    schemas.add(r.getString(1));
                }
            }
        }
        Metadata metadata = new Metadata(aDataSource, schemas, DataSources.getDataSourceSchema(aDataSource), DataSources.getDataSourceSqlDriver(aDataSource));
        metadata.fillTablesByDefaultSchema();
        return metadata;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public SqlDriver getSqlDriver() {
        return sqlDriver;
    }

    private String schemaName(String aQualifiedTableName) {
        int indexOfDot = aQualifiedTableName.indexOf(".");
        return indexOfDot != -1 ? aQualifiedTableName.substring(0, indexOfDot) : defaultSchema;
    }

    private static String tableName(String aQualifiedTableName) {
        int indexOfDot = aQualifiedTableName.indexOf(".");
        return indexOfDot != -1 ? aQualifiedTableName.substring(indexOfDot + 1) : aQualifiedTableName;
    }

    @Override
    public Optional<Map<String, JdbcColumn>> getTable(String aQualifiedTableName) throws SQLException {
        String schema = schemaName(aQualifiedTableName);
        if (schema != null && !schema.isEmpty() && schemas.contains(schema) && !schemasTablesColumns.containsKey(schema)) {
            fillTablesBySchema(schema);
        }
        return Optional
                .ofNullable(schemasTablesColumns.get(schema))
                .map(tables -> tables.get(tableName(aQualifiedTableName)));
    }

    public void refreshTable(String aQualifiedTableName) throws SQLException {
        String schema = schemaName(aQualifiedTableName);
        String table = tableName(aQualifiedTableName);
        Map<String, Map<String, JdbcColumn>> tables = schemasTablesColumns.computeIfAbsent(schema, sn -> new CaseInsensitiveMap<>());
        tables.put(table, queryTableColumns(schema, table));
    }

    public boolean containsTable(final String aQualifiedTableName) {
        return Optional
                .ofNullable(schemasTablesColumns.get(schemaName(aQualifiedTableName)))
                .map(tables -> tables.containsKey(tableName(aQualifiedTableName)))
                .orElse(false);
    }

    public boolean containsTableIndexes(String aQualifiedTableName) {
        return Optional
                .ofNullable(schemasTablesIndexes.get(schemaName(aQualifiedTableName)))
                .map(indexes -> indexes.containsKey(tableName(aQualifiedTableName)))
                .orElse(false);
    }

    /**
     * Fills tables metadata with fields, comments, keys (pk and fk) by connection
     * default schema.
     */
    private void fillTablesByDefaultSchema() throws SQLException {
        fillTablesBySchema(defaultSchema);
    }

    private Map<String, String> readTablesNames(String aSchema4Sql) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (ResultSet r = conn.getMetaData().getTables(null, aSchema4Sql, null, new String[]{JDBCPKS_TABLE_TYPE_TABLE, JDBCPKS_TABLE_TYPE_VIEW})) {
                Map<String, Integer> colIndicies = ColumnsIndicies.of(r.getMetaData());
                int colTableName = colIndicies.get(JDBCCOLS_TABLE_NAME);
                int colRemarks = colIndicies.get(JDBCCOLS_REMARKS);
                assert colTableName > 0;
                assert colRemarks > 0;
                Map<String, String> tNames = new CaseInsensitiveMap<>();
                while (r.next()) {
                    String tableName = r.getString(colTableName);
                    String remarks = r.getString(colRemarks);
                    tNames.put(tableName, remarks);
                }
                return tNames;
            }
        }
    }

    /**
     * Fills tables metadata with fields, comments, keys (pk and fk).
     *
     * @param aSchema A schema for witch we should achieve metadata information.
     *                If it is null, connection default schema is used
     */
    private void fillTablesBySchema(String aSchema) throws SQLException {
        schemasTablesColumns.put(aSchema, querySchemaColumns(aSchema));
    }

    /**
     * Fills indexes metadata.
     *
     * @param aSchema A schema for which we should achieve metadata information.
     *                If it is null, connection default schema is used
     * @param aTable  Table indexes to be fetched for.
     */
    public void fillIndexes(final String aSchema, final String aTable) {
        final Map<String, Map<String, TableIndex>> tablesIndexes = schemasTablesIndexes.computeIfAbsent(aSchema, sn -> new CaseInsensitiveMap<>());
        tablesIndexes.computeIfAbsent(aTable, tableName -> {
            try {
                return queryTableIndexes(aSchema, tableName);
            } catch (SQLException ex) {
                throw new UncheckedSQLException(ex);
            }
        });
    }

    private Map<String, JdbcColumn> queryTableColumns(String aSchema, String aTable) throws SQLException {
        return queryTablesColumns(aSchema, aTable).getOrDefault(aTable, new CaseInsensitiveMap<>());
    }

    private Map<String, Map<String, JdbcColumn>> querySchemaColumns(String aSchema) throws SQLException {
        return queryTablesColumns(aSchema, null);
    }

    private Map<String, Map<String, JdbcColumn>> queryTablesColumns(String aSchema, String aTableNamePattern) throws SQLException {
        SqlDriver sqlDriver = getSqlDriver();
        Map<String, Map<String, JdbcColumn>> columns;
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet r = meta.getColumns(null, aSchema, aTableNamePattern, null)) {
                columns = readTablesColumns(r, meta, aSchema, sqlDriver);
            }
            if (aSchema != null && !aSchema.isEmpty() && columns.isEmpty()) {
                try (ResultSet r = meta.getColumns(null, aSchema.toLowerCase(), aTableNamePattern != null ? aTableNamePattern.toLowerCase() : null, null)) {
                    columns = readTablesColumns(r, meta, aSchema.toLowerCase(), sqlDriver);
                }
            }
            if (aSchema != null && !aSchema.isEmpty() && columns.isEmpty()) {
                try (ResultSet r = meta.getColumns(null, aSchema.toUpperCase(), aTableNamePattern != null ? aTableNamePattern.toUpperCase() : null, null)) {
                    columns = readTablesColumns(r, meta, aSchema.toUpperCase(), sqlDriver);
                }
            }
            // columns.setTableDescription(aTablesDescriptions.get(tn));
            return columns;
        }
    }

    private static Map<String, Map<String, JdbcColumn>> readTablesColumns(ResultSet r, DatabaseMetaData meta, String schema, SqlDriver sqlDriver) throws SQLException {
        Map<String, Map<String, PrimaryKey>> tablesPks = new CaseInsensitiveMap<>();
        Map<String, Map<String, ForeignKey>> tablesFks = new CaseInsensitiveMap<>();
        Map<String, Map<String, JdbcColumn>> tablesColumns = new CaseInsensitiveMap<>();
        if (r != null) {
            Map<String, Integer> colIndicies = ColumnsIndicies.of(r.getMetaData());
            int colTableName = colIndicies.get(JDBCCOLS_TABLE_NAME);
            int colColumnName = colIndicies.get(JDBCCOLS_COLUMN_NAME);
            int colRemarks = colIndicies.get(JDBCCOLS_REMARKS);
            int colDataType = colIndicies.get(JDBCCOLS_DATA_TYPE);
            int colTypeName = colIndicies.get(JDBCCOLS_TYPE_NAME);
            int colColumnSize = colIndicies.get(JDBCCOLS_COLUMN_SIZE);
            int colDecimalDigits = colIndicies.get(JDBCCOLS_DECIMAL_DIGITS);
            int colNumPrecRadix = colIndicies.get(JDBCCOLS_NUM_PREC_RADIX);
            int colNullable = colIndicies.get(JDBCCOLS_NULLABLE);
            while (r.next()) {
                String fTableName = r.getString(colTableName);
                Map<String, JdbcColumn> columns = tablesColumns.computeIfAbsent(fTableName, tn -> new CaseInsensitiveMap<>());
                String fName = r.getString(colColumnName);
                String fDescription = r.getString(colRemarks);
                String rdbmsTypeName = r.getString(colTypeName);
                int jdbcType = r.getInt(colDataType);
                int size = r.getInt(colColumnSize);
                int scale = r.getInt(colDecimalDigits);
                int precision = r.getInt(colNumPrecRadix);
                int nullable = r.getInt(colNullable);
                //
                int resolvedSize = sqlDriver.getTypesResolver().resolveSize(rdbmsTypeName, size);

                Map<String, PrimaryKey> pks = tablesPks.computeIfAbsent(fTableName, tn -> {
                    try {
                        return queryTablePrimaryKeys(meta, schema, tn);
                    } catch (SQLException ex) {
                        throw new UncheckedSQLException(ex);
                    }
                });
                Map<String, ForeignKey> fks = tablesFks.computeIfAbsent(fTableName, tn -> {
                    try {
                        return queryTableForeignKeys(meta, schema, tn);
                    } catch (SQLException ex) {
                        throw new UncheckedSQLException(ex);
                    }
                });

                columns.put(fName, new JdbcColumn(
                        fName.toLowerCase(),
                        fDescription,
                        fName,
                        fTableName,
                        rdbmsTypeName,
                        nullable == ResultSetMetaData.columnNullable,
                        pks.containsKey(fName),
                        fks.get(fName),
                        resolvedSize,
                        scale,
                        precision,
                        true,
                        schema,
                        jdbcType
                ));
            }
        }
        return tablesColumns;
    }

    private static Map<String, PrimaryKey> queryTablePrimaryKeys(DatabaseMetaData meta, String schema, String table) throws SQLException {
        try (ResultSet r = meta.getPrimaryKeys(null, schema, table)) {
            return readTablesPrimaryKeys(r).getOrDefault(table, new CaseInsensitiveMap<>());
        }
    }

    private static Map<String, ForeignKey> queryTableForeignKeys(DatabaseMetaData meta, String schema, String table) throws SQLException {
        try (ResultSet r = meta.getImportedKeys(null, schema, table)) {
            return readTablesForeignKeys(r).getOrDefault(table, new CaseInsensitiveMap<>());
        }
    }

    private static Map<String, Map<String, PrimaryKey>> readTablesPrimaryKeys(ResultSet r) throws SQLException {
        Map<String, Map<String, PrimaryKey>> tablesKeys = new CaseInsensitiveMap<>();
        if (r != null) {
            Map<String, Integer> colsIndicies = ColumnsIndicies.of(r.getMetaData());
            int colTableSchem = colsIndicies.get(JDBCPKS_TABLE_SCHEM);
            int colTableName = colsIndicies.get(JDBCPKS_TABLE_NAME);
            int colColumnName = colsIndicies.get(JDBCPKS_COLUMN_NAME);
            int colConstraintName = colsIndicies.get(JDBCPKS_CONSTRAINT_NAME);
            while (r.next()) {
                String lpkSchema = r.getString(colTableSchem);
                String lpkTableName = r.getString(colTableName);
                String lpkField = r.getString(colColumnName);
                String lpkName = r.getString(colConstraintName);
                Map<String, PrimaryKey> tableKeys = tablesKeys.computeIfAbsent(lpkTableName, tn -> new CaseInsensitiveMap<>());
                tableKeys.put(lpkTableName, new PrimaryKey(lpkSchema, lpkTableName, lpkField, lpkName));
            }
        }
        return tablesKeys;
    }

    private static Map<String, Map<String, ForeignKey>> readTablesForeignKeys(ResultSet r) throws SQLException {
        Map<String, Map<String, ForeignKey>> tablesKeys = new CaseInsensitiveMap<>();
        if (r != null) {
            Map<String, Integer> colsIndicies = ColumnsIndicies.of(r.getMetaData());
            int colFkTableSchem = colsIndicies.get(JDBCFKS_FKTABLE_SCHEM);
            int colFkTableName = colsIndicies.get(JDBCFKS_FKTABLE_NAME);
            int colFkColumnName = colsIndicies.get(JDBCFKS_FKCOLUMN_NAME);
            int colFkName = colsIndicies.get(JDBCFKS_FK_NAME);
            int colFkUpdateRule = colsIndicies.get(JDBCFKS_FKUPDATE_RULE);
            int colFkDeleteRule = colsIndicies.get(JDBCFKS_FKDELETE_RULE);
            int colFkDeferrability = colsIndicies.get(JDBCFKS_FKDEFERRABILITY);
            //
            int colFkPkTableSchem = colsIndicies.get(JDBCFKS_FKPKTABLE_SCHEM);
            int colFkPkTableName = colsIndicies.get(JDBCFKS_FKPKTABLE_NAME);
            int colFkPkColumnName = colsIndicies.get(JDBCFKS_FKPKCOLUMN_NAME);
            int colFkPkName = colsIndicies.get(JDBCFKS_FKPK_NAME);
            while (r.next()) {
                String lfkSchema = r.getString(colFkTableSchem);
                String lfkTableName = r.getString(colFkTableName);
                String lfkField = r.getString(colFkColumnName);
                String lfkName = r.getString(colFkName);
                short lfkUpdateRule = r.getShort(colFkUpdateRule);
                short lfkDeleteRule = r.getShort(colFkDeleteRule);
                short lfkDeferability = r.getShort(colFkDeferrability);
                //
                String lpkSchema = r.getString(colFkPkTableSchem);
                String lpkTableName = r.getString(colFkPkTableName);
                String lpkField = r.getString(colFkPkColumnName);
                String lpkName = r.getString(colFkPkName);
                //
                Map<String, ForeignKey> tableKeys = tablesKeys.computeIfAbsent(lfkTableName, tn -> new CaseInsensitiveMap<>());
                tableKeys.put(lfkField, new ForeignKey(lfkSchema, lfkTableName, lfkField, lfkName,
                        ForeignKey.ForeignKeyRule.valueOf(lfkUpdateRule), ForeignKey.ForeignKeyRule.valueOf(lfkDeleteRule), lfkDeferability == 5,
                        lpkSchema, lpkTableName, lpkField, lpkName));
            }
        }
        return tablesKeys;
    }

    public Optional<Map<String, TableIndex>> getTableIndexes(String aQualifiedTableName) {
        Map<String, Map<String, TableIndex>> tablesIndexes = schemasTablesIndexes.get(schemaName(aQualifiedTableName));
        return Optional.ofNullable(tablesIndexes.get(tableName(aQualifiedTableName)));
    }

    private Map<String, TableIndex> queryTableIndexes(String aSchema, String aTable) throws SQLException {
        Map<String, TableIndex> indexSpecs = new CaseInsensitiveMap<>();
        String schema4Sql = aSchema != null && !aSchema.isEmpty() ? aSchema : defaultSchema;
        try (Connection conn = dataSource.getConnection()) {
            try (ResultSet r = conn.getMetaData().getIndexInfo(null, schema4Sql, aTable, false, false)) {
                Map<String, Integer> idxs = ColumnsIndicies.of(r.getMetaData());
                int colIndexName = idxs.get(JDBCIDX_INDEX_NAME);
                int colNonUnique = idxs.get(JDBCIDX_NON_UNIQUE);
                int colType = idxs.get(JDBCIDX_TYPE);
                //int colTableName = idxs.get(JDBCIDX_TABLE_NAME);
                int colColumnName = idxs.get(JDBCIDX_COLUMN_NAME);
                int colAscOrDesc = idxs.get(JDBCIDX_ASC_OR_DESC);
                int colOrdinalPosition = idxs.get(JDBCIDX_ORDINAL_POSITION);
                String idxName = null;
                Set<TableIndex.Column> columns = new LinkedHashSet<>();
                boolean clustered = false;
                boolean unique = false;
                boolean hashed = false;
                while (r.next()) {
                    //String tableName = r.getString(JDBCIDX_TABLE_NAME);
                    idxName = r.getString(colIndexName);
                    if (!r.wasNull()) {
                        unique = r.getBoolean(colNonUnique);
                        short type = r.getShort(colType);
                        switch (type) {
                            case DatabaseMetaData.tableIndexClustered:
                                clustered = true;
                                break;
                            case DatabaseMetaData.tableIndexHashed:
                                hashed = true;
                                break;
                            case DatabaseMetaData.tableIndexStatistic:
                                break;
                            case DatabaseMetaData.tableIndexOther:
                                break;
                        }
                        String sColumnName = r.getString(colColumnName);
                        if (!r.wasNull()) {
                            String sAsc = r.getString(colAscOrDesc);
                            if (!r.wasNull()) {
                                sAsc = null;
                            }
                            short sPosition = r.getShort(colOrdinalPosition);
                            columns.add(new TableIndex.Column(
                                    sColumnName,
                                    sAsc == null || sAsc.toLowerCase().equals("a"),
                                    (int) sPosition
                            ));
                        }
                    }
                }
                if (idxName != null) {
                    indexSpecs.put(idxName, new TableIndex(idxName, clustered, hashed, unique, columns));
                }
            }
        }
        return indexSpecs;
    }

}
