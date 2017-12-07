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


    private static class CaseInsensitiveMap<V> extends HashMap<String, V> {

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
    // Schema, Table, Field
    private final Map<String, Map<String, Map<String, JdbcColumn>>> schemasTablesFields = new ConcurrentCaseInsensitiveMap<>();
    // Schema, Table, Index
    private final Map<String, Map<String, Map<String, TableIndex>>> schemasTablesIndexes = new ConcurrentCaseInsensitiveMap<>();
    private final String dataSourceSchema;
    private final SqlDriver dataSourceDriver;

    private Metadata(DataSource aDataSource, String aDataSourceSchema, SqlDriver aDataSourceDriver) {
        dataSource = aDataSource;
        dataSourceSchema = aDataSourceSchema;
        dataSourceDriver = aDataSourceDriver;
    }

    public static Metadata of(DataSource aDataSource) throws Exception {
        Metadata metadata = new Metadata(aDataSource, DataSources.getDataSourceSchema(aDataSource), DataSources.getDataSourceSqlDriver(aDataSource));
        metadata.fillTablesByDataSourceSchema();
        return metadata;
    }

    public String getDataSourceSchema() {
        return dataSourceSchema;
    }

    public SqlDriver getDataSourceSqlDriver() {
        return dataSourceDriver;
    }

    private String schemaName(String aQualifiedTableName) {
        int indexOfDot = aQualifiedTableName.indexOf(".");
        return indexOfDot != -1 ? aQualifiedTableName.substring(0, indexOfDot) : dataSourceSchema;
    }

    private static String tableName(String aQualifiedTableName) {
        int indexOfDot = aQualifiedTableName.indexOf(".");
        return indexOfDot != -1 ? aQualifiedTableName.substring(indexOfDot + 1) : aQualifiedTableName;
    }

    @Override
    public Optional<Map<String, JdbcColumn>> getTable(String aQualifiedTableName) throws Exception {
        return Optional
                .ofNullable(schemasTablesFields.get(schemaName(aQualifiedTableName)))
                .map(tables -> tables.get(tableName(aQualifiedTableName)));
    }

    public void refreshTable(String aQualifiedTableName) throws Exception {
        String schema = schemaName(aQualifiedTableName);
        schemasTablesFields.computeIfAbsent(schema, sn -> new CaseInsensitiveMap<>());
        queryTablesFields(schema, Set.of(tableName(aQualifiedTableName)));
    }

    public boolean containsTable(final String aQualifiedTableName) throws Exception {
        return Optional
                .ofNullable(schemasTablesFields.get(schemaName(aQualifiedTableName)))
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
     * @throws Exception
     */
    public final void fillTablesByDataSourceSchema() throws Exception {
        fillTablesBySchema(dataSourceSchema);
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
     * @throws Exception
     */
    public void fillTablesBySchema(String aSchema) throws Exception {
        Map<String, String> tablesNames = readTablesNames(aSchema);
        if (aSchema != null && !aSchema.isEmpty() && tablesNames.isEmpty()) {
            tablesNames = readTablesNames(aSchema.toLowerCase());
        }
        if (aSchema != null && !aSchema.isEmpty() && tablesNames.isEmpty()) {
            tablesNames = readTablesNames(aSchema.toUpperCase());
        }
        schemasTablesFields.put(aSchema, queryTablesFields(aSchema, tablesNames.keySet()));
    }

    /**
     * Fills indexes metadata.
     *
     * @param aSchema A schema for which we should achieve metadata information.
     *                If it is null, connection default schema is used
     * @param aTable
     * @throws Exception
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

    private static void resolveKeys(Map<String, Map<String, JdbcColumn>> aTablesFields, Map<String, TableKeys> aTablesKeys) throws Exception {
        aTablesFields.keySet().stream().forEach((String lTableName) -> {
            Map<String, JdbcColumn> fields = aTablesFields.get(lTableName);
            TableKeys keys = aTablesKeys.get(lTableName);
            if (keys != null) {
                keys.getPks().entrySet().stream().forEach((Map.Entry<String, PrimaryKey> pkEntry) -> {
                    Field f = fields.get(pkEntry.getKey());
                    if (f != null) {
                        f.setPk(true);
                    }
                });
                keys.getFks().entrySet().stream().forEach((Map.Entry<String, ForeignKey> fkEntry) -> {
                    Field f = fields.get(fkEntry.getKey());
                    if (f != null) {
                        f.setFk(fkEntry.getValue());
                    }
                });
            }
        });
    }

    private Map<String, Map<String, JdbcColumn>> queryTablesFields(String aSchema, Set<String> aTables) throws Exception {
        SqlDriver sqlDriver = getDataSourceSqlDriver();
        Map<String, Map<String, JdbcColumn>> columns;
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet r = meta.getColumns(null, aSchema, null, null)) {
                columns = readTablesColumns(r, aSchema, sqlDriver);
            }
            if (aSchema != null && !aSchema.isEmpty() && columns.isEmpty()) {
                aSchema = aSchema.toLowerCase();
                try (ResultSet r = meta.getColumns(null, aSchema, null, null)) {
                    columns = readTablesColumns(r, aSchema, sqlDriver);
                }
            }
            if (aSchema != null && !aSchema.isEmpty() && columns.isEmpty()) {
                aSchema = aSchema.toUpperCase();
                try (ResultSet r = meta.getColumns(null, aSchema, null, null)) {
                    columns = readTablesColumns(r, aSchema, sqlDriver);
                }
            }
            Map<String, TableKeys> keys = new CaseInsensitiveMap<>();
            for (String aTable : aTables) {
                try (ResultSet r = meta.getPrimaryKeys(null, aSchema, aTable)) {
                    TableKeys tableKeys = readTablesPrimaryKeys(r);
                    keys.put(aTable, tableKeys);
                }
                try (ResultSet r = meta.getImportedKeys(null, aSchema, aTable)) {
                    readTablesForeignKeys(r, keys);
                }
            }
            resolveKeys(columns, keys);
            // columns.setTableDescription(aTablesDescriptions.get(tn));
            return columns;
        }
    }

    private static Map<String, Map<String, JdbcColumn>> readTablesColumns(ResultSet r, String aSchema, SqlDriver sqlDriver) throws Exception {
        Map<String, Map<String, JdbcColumn>> tablesFields = new CaseInsensitiveMap<>();
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
                Map<String, JdbcColumn> fields = tablesFields.computeIfAbsent(fTableName, tn -> new LinkedHashMap<>());
                String fName = r.getString(JDBCCOLS_COLUMN_INDEX);
                String fDescription = r.getString(JDBCCOLS_REMARKS_INDEX);
                JdbcColumn field = new JdbcColumn();
                field.setName(fName.toLowerCase());
                field.setDescription(fDescription);
                field.setOriginalName(fName);
                String rdbmsTypeName = r.getString(JDBCCOLS_TYPE_NAME_INDEX);
                field.setType(rdbmsTypeName);
                int jdbcType = r.getInt(JDBCCOLS_DATA_TYPE_INDEX);
                field.setJdbcType(jdbcType);
                int size = r.getInt(JDBCCOLS_COLUMN_SIZE_INDEX);
                field.setSize(size);
                int scale = r.getInt(JDBCCOLS_DECIMAL_DIGITS_INDEX);
                field.setScale(scale);
                int precision = r.getInt(JDBCCOLS_NUM_PREC_RADIX_INDEX);
                field.setPrecision(precision);
                int nullable = r.getInt(JDBCCOLS_NULLABLE_INDEX);
                field.setNullable(nullable == ResultSetMetaData.columnNullable);
                field.setSchemaName(aSchema);
                field.setTableName(fTableName);
                //
                sqlDriver.getTypesResolver().resolveSize(field);
                fields.put(field.getName(), field);
            }
        }
        return tablesFields;
    }

    private static TableKeys readTablesPrimaryKeys(ResultSet r) throws Exception {
        TableKeys tableKeys = new TableKeys();
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
                //
                tableKeys.addPrimaryKey(lpkSchema, lpkTableName, lpkField, lpkName);
            }
        }
        return tableKeys;
    }

    private static void readTablesForeignKeys(ResultSet r, Map<String, TableKeys> tabledKeys) throws Exception {
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
                TableKeys tableKeys = tabledKeys.computeIfAbsent(lfkTableName, tn -> new TableKeys());
                tableKeys.addForeignKey(lfkSchema, lfkTableName, lfkField, lfkName, ForeignKey.ForeignKeyRule.valueOf(lfkUpdateRule), ForeignKey.ForeignKeyRule.valueOf(lfkDeleteRule), lfkDeferability == 5, lpkSchema, lpkTableName, lpkField, lpkName);
            }
        }
    }

    public Optional<Map<String, TableIndex>> getTableIndexes(String aQualifiedTableName) throws Exception {
        Map<String, Map<String, TableIndex>> tablesIndexes = schemasTablesIndexes.get(schemaName(aQualifiedTableName));
        return Optional.ofNullable(tablesIndexes.get(tableName(aQualifiedTableName)));
    }

    private Map<String, TableIndex> queryTableIndexes(String aSchema, String aTable) throws Exception {
        Map<String, TableIndex> indexSpecs = new CaseInsensitiveMap<>();
        String schema4Sql = aSchema != null && !aSchema.isEmpty() ? aSchema : dataSourceSchema;
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
                    while (r.next()) {
                        //String tableName = r.getString(JDBCIDX_TABLE_NAME);
                        String idxName = r.getString(JDBCIDX_INDEX_NAME);
                        if (!r.wasNull()) {
                            TableIndex idxSpec = indexSpecs.computeIfAbsent(idxName, in -> new TableIndex(in));
                            boolean isUnique = r.getBoolean(JDBCIDX_NON_UNIQUE);
                            short type = r.getShort(JDBCIDX_TYPE);
                            idxSpec.setUnique(isUnique);
                            idxSpec.setClustered(false);
                            idxSpec.setHashed(false);
                            switch (type) {
                                case DatabaseMetaData.tableIndexClustered:
                                    idxSpec.setClustered(true);
                                    break;
                                case DatabaseMetaData.tableIndexHashed:
                                    idxSpec.setHashed(true);
                                    break;
                                case DatabaseMetaData.tableIndexStatistic:
                                    break;
                                case DatabaseMetaData.tableIndexOther:
                                    break;
                            }
                            String sColumnName = r.getString(JDBCIDX_COLUMN_NAME);
                            if (!r.wasNull()) {
                                TableIndexColumn column = idxSpec.getColumn(sColumnName);
                                if (column == null) {
                                    column = new TableIndexColumn(sColumnName, true);
                                    idxSpec.addColumn(column);
                                }
                                String sAsc = r.getString(JDBCIDX_ASC_OR_DESC);
                                if (!r.wasNull()) {
                                    column.setAscending(sAsc.toLowerCase().equals("a"));
                                }
                                short sPosition = r.getShort(JDBCIDX_ORDINAL_POSITION);
                                column.setOrdinalPosition((int) sPosition);
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                Logger.getLogger(Metadata.class.getName()).log(Level.WARNING, ex.toString());
            }
        }
        return indexSpecs;
    }

}
