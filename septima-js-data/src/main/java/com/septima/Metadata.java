package com.septima;

import com.septima.metadata.*;
import com.septima.dataflow.ColumnsIndicies;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * @author mg
 */
public class Metadata implements StatementsGenerator.TablesContainer {

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

    public static Metadata of(DataSource aDataSource) throws Exception {
        Set<String> schemas = new CaseInsensitiveSet();
        try (Connection conn = aDataSource.getConnection()) {
            try (ResultSet r = conn.getMetaData().getSchemas()) {
                while (r.next()) {
                    schemas.add(r.getString(1));
                }
            }
        }
        Metadata metadata = new Metadata(aDataSource, schemas, DataSources.getDataSourceSchema(aDataSource), DataSources.getDataSourceSqlDriver(aDataSource));
        metadata.fillTablesByDataSourceSchema();
        return metadata;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public SqlDriver getDataSourceSqlDriver() {
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
    public Optional<Map<String, JdbcColumn>> getTable(String aQualifiedTableName) throws Exception {
        String schema = schemaName(aQualifiedTableName);
        if (schema != null && !schema.isEmpty() && schemas.contains(schema)) {
            fillTablesBySchema(schema);
        }
        return Optional
                .ofNullable(schemasTablesColumns.get(schema))
                .map(tables -> tables.get(tableName(aQualifiedTableName)));
    }

    public void refreshTable(String aQualifiedTableName) throws Exception {
        String schema = schemaName(aQualifiedTableName);
        String table = tableName(aQualifiedTableName);
        Map<String, Map<String, JdbcColumn>> tables = schemasTablesColumns.computeIfAbsent(schema, sn -> new CaseInsensitiveMap<>());
        tables.put(table, queryTableColumns(schema, table));
    }

    public boolean containsTable(final String aQualifiedTableName) throws Exception {
        return Optional
                .ofNullable(schemasTablesColumns.get(schemaName(aQualifiedTableName)))
                .map(tables -> tables.containsKey(tableName(aQualifiedTableName)))
                .orElse(false);
    }

    public boolean containsTableIndexes(String aQualifiedTableName) throws Exception {
        return Optional
                .ofNullable(schemasTablesIndexes.get(schemaName(aQualifiedTableName)))
                .map(indexes -> indexes.containsKey(tableName(aQualifiedTableName)))
                .orElse(false);
    }

    /**
     * Fills tables metadata with fields, comments, keys (pk and fk) by connection
     * default schema.
     *
     * @throws Exception If some error occurs while database communication
     */
    public final void fillTablesByDataSourceSchema() throws Exception {
        fillTablesBySchema(defaultSchema);
    }

    private Map<String, String> readTablesNames(String aSchema4Sql) throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            try (ResultSet r = conn.getMetaData().getTables(null, aSchema4Sql, null, new String[]{Constants.JDBCPKS_TABLE_TYPE_TABLE, Constants.JDBCPKS_TABLE_TYPE_VIEW})) {
                Map<String, Integer> colIndicies = ColumnsIndicies.of(r.getMetaData());
                int colIndex = colIndicies.get(Constants.JDBCCOLS_TABLE_NAME);
                int colRemarks = colIndicies.get(Constants.JDBCCOLS_REMARKS);
                assert colIndex > 0;
                assert colRemarks > 0;
                Map<String, String> tNames = new CaseInsensitiveMap<>();
                while (r.next()) {
                    String lTableName = r.getString(colIndex);
                    String lRemarks = r.getString(colRemarks);
                    tNames.put(lTableName, lRemarks);
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
     * @throws Exception If some error occurs while database communication
     */
    public void fillTablesBySchema(String aSchema) throws Exception {
        schemasTablesColumns.put(aSchema, querySchemaColumns(aSchema));
    }

    /**
     * Fills indexes metadata.
     *
     * @param aSchema A schema for which we should achieve metadata information.
     *                If it is null, connection default schema is used
     * @param aTable
     * @throws Exception If some error occurs while database communication
     */
    public void fillIndexes(final String aSchema, final String aTable) throws Exception {
        final Map<String, Map<String, TableIndex>> tablesIndexes = schemasTablesIndexes.computeIfAbsent(aSchema, sn -> new CaseInsensitiveMap<>());
        tablesIndexes.computeIfAbsent(aTable, tableName -> {
            try {
                return queryTableIndexes(aSchema, tableName);
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
    }

    private Map<String, JdbcColumn> queryTableColumns(String aSchema, String aTable) throws Exception {
        return queryTablesColumns(aSchema, aTable).getOrDefault(aTable, new CaseInsensitiveMap<>());
    }

    private Map<String, Map<String, JdbcColumn>> querySchemaColumns(String aSchema) throws Exception {
        return queryTablesColumns(aSchema, null);
    }

    private Map<String, Map<String, JdbcColumn>> queryTablesColumns(String aSchema, String aTableNamePattern) throws Exception {
        SqlDriver sqlDriver = getDataSourceSqlDriver();
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

    private static Map<String, Map<String, JdbcColumn>> readTablesColumns(ResultSet r, DatabaseMetaData meta, String schema, SqlDriver sqlDriver) throws Exception {
        Map<String, Map<String, PrimaryKey>> tablesPks = new CaseInsensitiveMap<>();
        Map<String, Map<String, ForeignKey>> tablesFks = new CaseInsensitiveMap<>();
        Map<String, Map<String, JdbcColumn>> tablesColumns = new CaseInsensitiveMap<>();
        if (r != null) {
            Map<String, Integer> colIndicies = ColumnsIndicies.of(r.getMetaData());
            int JDBCCOLS_TABLE_INDEX = colIndicies.get(Constants.JDBCCOLS_TABLE_NAME);
            int JDBCCOLS_COLUMN_INDEX = colIndicies.get(Constants.JDBCCOLS_COLUMN_NAME);
            int JDBCCOLS_REMARKS_INDEX = colIndicies.get(Constants.JDBCCOLS_REMARKS);
            int JDBCCOLS_DATA_TYPE_INDEX = colIndicies.get(Constants.JDBCCOLS_DATA_TYPE);
            int JDBCCOLS_TYPE_NAME_INDEX = colIndicies.get(Constants.JDBCCOLS_TYPE_NAME);
            int JDBCCOLS_COLUMN_SIZE_INDEX = colIndicies.get(Constants.JDBCCOLS_COLUMN_SIZE);
            int JDBCCOLS_DECIMAL_DIGITS_INDEX = colIndicies.get(Constants.JDBCCOLS_DECIMAL_DIGITS);
            int JDBCCOLS_NUM_PREC_RADIX_INDEX = colIndicies.get(Constants.JDBCCOLS_NUM_PREC_RADIX);
            int JDBCCOLS_NULLABLE_INDEX = colIndicies.get(Constants.JDBCCOLS_NULLABLE);
            while (r.next()) {
                String fTableName = r.getString(JDBCCOLS_TABLE_INDEX);
                Map<String, JdbcColumn> columns = tablesColumns.computeIfAbsent(fTableName, tn -> new CaseInsensitiveMap<>());
                String fName = r.getString(JDBCCOLS_COLUMN_INDEX);
                String fDescription = r.getString(JDBCCOLS_REMARKS_INDEX);
                String rdbmsTypeName = r.getString(JDBCCOLS_TYPE_NAME_INDEX);
                int jdbcType = r.getInt(JDBCCOLS_DATA_TYPE_INDEX);
                int size = r.getInt(JDBCCOLS_COLUMN_SIZE_INDEX);
                int scale = r.getInt(JDBCCOLS_DECIMAL_DIGITS_INDEX);
                int precision = r.getInt(JDBCCOLS_NUM_PREC_RADIX_INDEX);
                int nullable = r.getInt(JDBCCOLS_NULLABLE_INDEX);
                //
                int resolvedSize = sqlDriver.getTypesResolver().resolveSize(rdbmsTypeName, size);

                Map<String, PrimaryKey> pks = tablesPks.computeIfAbsent(fTableName, tn -> {
                    try {
                        return queryTablePrimaryKeys(meta, schema, tn);
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                });
                Map<String, ForeignKey> fks = tablesFks.computeIfAbsent(fTableName, tn -> {
                    try {
                        return queryTableForeignKeys(meta, schema, tn);
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
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

    private static Map<String, PrimaryKey> queryTablePrimaryKeys(DatabaseMetaData meta, String schema, String table) throws Exception {
        try (ResultSet r = meta.getPrimaryKeys(null, schema, table)) {
            return readTablesPrimaryKeys(r).getOrDefault(table, new CaseInsensitiveMap<>());
        }
    }

    private static Map<String, ForeignKey> queryTableForeignKeys(DatabaseMetaData meta, String schema, String table) throws Exception {
        try (ResultSet r = meta.getImportedKeys(null, schema, table)) {
            return readTablesForeignKeys(r).getOrDefault(table, new CaseInsensitiveMap<>());
        }
    }

    private static Map<String, Map<String, PrimaryKey>> readTablesPrimaryKeys(ResultSet r) throws Exception {
        Map<String, Map<String, PrimaryKey>> tablesKeys = new CaseInsensitiveMap<>();
        if (r != null) {
            Map<String, Integer> colsIndicies = ColumnsIndicies.of(r.getMetaData());
            int JDBCPKS_TABLE_SCHEM_INDEX = colsIndicies.get(Constants.JDBCPKS_TABLE_SCHEM);
            int JDBCPKS_TABLE_NAME_INDEX = colsIndicies.get(Constants.JDBCPKS_TABLE_NAME);
            int JDBCPKS_COLUMN_NAME_INDEX = colsIndicies.get(Constants.JDBCPKS_COLUMN_NAME);
            int JDBCPKS_CONSTRAINT_NAME_INDEX = colsIndicies.get(Constants.JDBCPKS_CONSTRAINT_NAME);
            while (r.next()) {
                String lpkSchema = r.getString(JDBCPKS_TABLE_SCHEM_INDEX);
                String lpkTableName = r.getString(JDBCPKS_TABLE_NAME_INDEX);
                String lpkField = r.getString(JDBCPKS_COLUMN_NAME_INDEX);
                String lpkName = r.getString(JDBCPKS_CONSTRAINT_NAME_INDEX);
                Map<String, PrimaryKey> tableKeys = tablesKeys.computeIfAbsent(lpkTableName, tn -> new CaseInsensitiveMap<>());
                tableKeys.put(lpkTableName, new PrimaryKey(lpkSchema, lpkTableName, lpkField, lpkName));
            }
        }
        return tablesKeys;
    }

    private static Map<String, Map<String, ForeignKey>> readTablesForeignKeys(ResultSet r) throws Exception {
        Map<String, Map<String, ForeignKey>> tablesKeys = new CaseInsensitiveMap<>();
        if (r != null) {
            Map<String, Integer> colsIndicies = ColumnsIndicies.of(r.getMetaData());
            int JDBCFKS_FKTABLE_SCHEM_INDEX = colsIndicies.get(Constants.JDBCFKS_FKTABLE_SCHEM);
            int JDBCFKS_FKTABLE_NAME_INDEX = colsIndicies.get(Constants.JDBCFKS_FKTABLE_NAME);
            int JDBCFKS_FKCOLUMN_NAME_INDEX = colsIndicies.get(Constants.JDBCFKS_FKCOLUMN_NAME);
            int JDBCFKS_FK_NAME_INDEX = colsIndicies.get(Constants.JDBCFKS_FK_NAME);
            int JDBCFKS_FKUPDATE_RULE_INDEX = colsIndicies.get(Constants.JDBCFKS_FKUPDATE_RULE);
            int JDBCFKS_FKDELETE_RULE_INDEX = colsIndicies.get(Constants.JDBCFKS_FKDELETE_RULE);
            int JDBCFKS_FKDEFERRABILITY_INDEX = colsIndicies.get(Constants.JDBCFKS_FKDEFERRABILITY);
            //
            int JDBCFKS_FKPKTABLE_SCHEM_INDEX = colsIndicies.get(Constants.JDBCFKS_FKPKTABLE_SCHEM);
            int JDBCFKS_FKPKTABLE_NAME_INDEX = colsIndicies.get(Constants.JDBCFKS_FKPKTABLE_NAME);
            int JDBCFKS_FKPKCOLUMN_NAME_INDEX = colsIndicies.get(Constants.JDBCFKS_FKPKCOLUMN_NAME);
            int JDBCFKS_FKPK_NAME_INDEX = colsIndicies.get(Constants.JDBCFKS_FKPK_NAME);
            while (r.next()) {
                String lfkSchema = r.getString(JDBCFKS_FKTABLE_SCHEM_INDEX);
                String lfkTableName = r.getString(JDBCFKS_FKTABLE_NAME_INDEX);
                String lfkField = r.getString(JDBCFKS_FKCOLUMN_NAME_INDEX);
                String lfkName = r.getString(JDBCFKS_FK_NAME_INDEX);
                short lfkUpdateRule = r.getShort(JDBCFKS_FKUPDATE_RULE_INDEX);
                short lfkDeleteRule = r.getShort(JDBCFKS_FKDELETE_RULE_INDEX);
                short lfkDeferability = r.getShort(JDBCFKS_FKDEFERRABILITY_INDEX);
                //
                String lpkSchema = r.getString(JDBCFKS_FKPKTABLE_SCHEM_INDEX);
                String lpkTableName = r.getString(JDBCFKS_FKPKTABLE_NAME_INDEX);
                String lpkField = r.getString(JDBCFKS_FKPKCOLUMN_NAME_INDEX);
                String lpkName = r.getString(JDBCFKS_FKPK_NAME_INDEX);
                //
                Map<String, ForeignKey> tableKeys = tablesKeys.computeIfAbsent(lfkTableName, tn -> new CaseInsensitiveMap<>());
                tableKeys.put(lfkField, new ForeignKey(lfkSchema, lfkTableName, lfkField, lfkName,
                        ForeignKey.ForeignKeyRule.valueOf(lfkUpdateRule), ForeignKey.ForeignKeyRule.valueOf(lfkDeleteRule), lfkDeferability == 5,
                        lpkSchema, lpkTableName, lpkField, lpkName));
            }
        }
        return tablesKeys;
    }

    public Optional<Map<String, TableIndex>> getTableIndexes(String aQualifiedTableName) throws Exception {
        Map<String, Map<String, TableIndex>> tablesIndexes = schemasTablesIndexes.get(schemaName(aQualifiedTableName));
        return Optional.ofNullable(tablesIndexes.get(tableName(aQualifiedTableName)));
    }

    private Map<String, TableIndex> queryTableIndexes(String aSchema, String aTable) throws Exception {
        Map<String, TableIndex> indexSpecs = new CaseInsensitiveMap<>();
        String schema4Sql = aSchema != null && !aSchema.isEmpty() ? aSchema : defaultSchema;
        try (Connection conn = dataSource.getConnection()) {
            try {
                try (ResultSet r = conn.getMetaData().getIndexInfo(null, schema4Sql, aTable, false, false)) {
                    Map<String, Integer> idxs = ColumnsIndicies.of(r.getMetaData());
                    int JDBCIDX_INDEX_NAME = idxs.get(Constants.JDBCIDX_INDEX_NAME);
                    int JDBCIDX_NON_UNIQUE = idxs.get(Constants.JDBCIDX_NON_UNIQUE);
                    int JDBCIDX_TYPE = idxs.get(Constants.JDBCIDX_TYPE);
                    //int JDBCIDX_TABLE_NAME = idxs.get(Constants.JDBCIDX_TABLE_NAME);
                    int JDBCIDX_COLUMN_NAME = idxs.get(Constants.JDBCIDX_COLUMN_NAME);
                    int JDBCIDX_ASC_OR_DESC = idxs.get(Constants.JDBCIDX_ASC_OR_DESC);
                    int JDBCIDX_ORDINAL_POSITION = idxs.get(Constants.JDBCIDX_ORDINAL_POSITION);
                    String idxName = null;
                    Set<TableIndexColumn> columns = new LinkedHashSet<>();
                    boolean clustered = false;
                    boolean unique = false;
                    boolean hashed = false;
                    while (r.next()) {
                        //String tableName = r.getString(JDBCIDX_TABLE_NAME);
                        idxName = r.getString(JDBCIDX_INDEX_NAME);
                        if (!r.wasNull()) {
                            unique = r.getBoolean(JDBCIDX_NON_UNIQUE);
                            short type = r.getShort(JDBCIDX_TYPE);
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
                            String sColumnName = r.getString(JDBCIDX_COLUMN_NAME);
                            if (!r.wasNull()) {
                                String sAsc = r.getString(JDBCIDX_ASC_OR_DESC);
                                if (!r.wasNull()) {
                                    sAsc = null;
                                }
                                short sPosition = r.getShort(JDBCIDX_ORDINAL_POSITION);
                                columns.add(new TableIndexColumn(
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
            } catch (SQLException ex) {
                Logger.getLogger(Metadata.class.getName()).log(Level.WARNING, ex.toString());
            }
        }
        return indexSpecs;
    }

}
