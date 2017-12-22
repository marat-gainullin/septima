package com.septima;

import com.septima.dataflow.StatementsGenerator;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.PrimaryKey;
import com.septima.queries.CaseInsensitiveMap;
import com.septima.sqldrivers.SqlDriver;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author mg
 */
public class Metadata implements StatementsGenerator.TablesContainer {

    private static final String JDBC_FK_TABLE_SCHEMA = "FKTABLE_SCHEM";
    private static final String JDBC_FK_TABLE_NAME = "FKTABLE_NAME";
    private static final String JDBC_FK_COLUMN_NAME = "FKCOLUMN_NAME";
    private static final String JDBC_FK_CONSTRAINT_NAME = "FK_NAME";
    private static final String JDBC_FK_UPDATE_RULE = "UPDATE_RULE";
    private static final String JDBC_FK_DELETE_RULE = "DELETE_RULE";
    private static final String JDBC_FK_DEFERRABILITY = "DEFERRABILITY";
    private static final String JDBC_FK_PK_TABLE_SCHEMA = "PKTABLE_SCHEM";
    private static final String JDBC_FK_PK_TABLE_NAME = "PKTABLE_NAME";
    private static final String JDBC_FK_PK_COLUMN_NAME = "PKCOLUMN_NAME";
    private static final String JDBC_TABLE_SCHEMA = "TABLE_SCHEM";
    private static final String JDBC_TABLE_NAME = "TABLE_NAME";
    private static final String JDBC_COLUMN_NAME = "COLUMN_NAME";
    private static final String JDBC_REMARKS = "REMARKS";
    private static final String JDBC_DATA_TYPE = "DATA_TYPE";
    private static final String JDBC_TYPE_NAME = "TYPE_NAME";
    private static final String JDBC_COLUMN_SIZE = "COLUMN_SIZE";
    private static final String JDBC_DECIMAL_DIGITS = "DECIMAL_DIGITS";
    private static final String JDBC_NUMBER_PRECISION_RADIX = "NUM_PREC_RADIX";
    private static final String JDBC_NULLABLE = "NULLABLE";
    private static final String JDBC_PK_CONSTRAINT_NAME = "PK_NAME";

    private static class CaseInsensitiveSet extends AbstractSet<String> {

        private static String keyToLowerCase(String aKey) {
            return aKey != null ? aKey.toLowerCase() : null;
        }

        private Set<String> delegate;

        CaseInsensitiveSet(Set<String> aDelegate) {
            super();
            Objects.requireNonNull(aDelegate, "aDelegate is required argument");
            delegate = aDelegate;
        }

        @Override
        public Iterator<String> iterator() {
            return delegate.iterator();
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean add(String s) {
            return delegate.add(keyToLowerCase(s));
        }

        @Override
        public boolean addAll(Collection<? extends String> c) {
            return delegate.addAll(c.stream()
                    .map(CaseInsensitiveSet::keyToLowerCase)
                    .collect(Collectors.toSet()));
        }

        @Override
        public boolean remove(Object o) {
            return delegate.remove(keyToLowerCase((String) o));
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return delegate.removeAll(c.stream()
                    .map(o -> keyToLowerCase((String) o))
                    .collect(Collectors.toSet()));
        }

        @Override
        public boolean contains(Object o) {
            return delegate.contains(keyToLowerCase((String) o));
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return c.stream()
                    .allMatch(e -> delegate.contains(keyToLowerCase((String) e)));
        }

    }

    private final DataSource dataSource;
    private final Set<String> schemas;
    // Schema, Table, Field
    private final Map<String, Map<String, Map<String, JdbcColumn>>> schemasTablesColumns = new CaseInsensitiveMap<>(new ConcurrentHashMap<>());
    private final String defaultSchema;
    private final SqlDriver sqlDriver;

    private Metadata(DataSource aDataSource, Set<String> aSchemas, String aDefaultSchema, SqlDriver aSqlDriver) {
        schemas = aSchemas;
        dataSource = aDataSource;
        defaultSchema = aDefaultSchema;
        sqlDriver = aSqlDriver;
    }

    public static Metadata of(DataSource aDataSource) throws SQLException {
        Objects.requireNonNull(aDataSource, "aDataSource is required argument");
        Set<String> schemas = new CaseInsensitiveSet(new HashSet<>());
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
    public Optional<Map<String, JdbcColumn>> getTableColumns(String aQualifiedTableName) throws SQLException {
        String schema = schemaName(aQualifiedTableName);
        if (schema != null && !schema.isEmpty() && schemas.contains(schema) && !schemasTablesColumns.containsKey(schema)) {
            fillTablesBySchema(schema);
        }
        return Optional
                .ofNullable(schemasTablesColumns.get(schema))
                .map(tables -> tables.get(tableName(aQualifiedTableName)))
                .map(Collections::unmodifiableMap);
    }

    public void refreshTable(String aQualifiedTableName) throws SQLException {
        String schema = schemaName(aQualifiedTableName);
        String table = tableName(aQualifiedTableName);
        Map<String, Map<String, JdbcColumn>> tables = schemasTablesColumns.computeIfAbsent(schema, sn -> new CaseInsensitiveMap<>(new LinkedHashMap<>()));
        tables.put(table, queryTableColumns(schema, table));
    }

    public boolean containsTable(final String aQualifiedTableName) {
        return Optional
                .ofNullable(schemasTablesColumns.get(schemaName(aQualifiedTableName)))
                .map(tables -> tables.containsKey(tableName(aQualifiedTableName)))
                .orElse(false);
    }

    /**
     * Fills tables metadata with fields, comments, keys (pk and fk) by connection
     * default schema.
     */
    private void fillTablesByDefaultSchema() throws SQLException {
        fillTablesBySchema(defaultSchema);
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

    private Map<String, JdbcColumn> queryTableColumns(String aSchema, String aTable) throws SQLException {
        return queryTablesColumns(aSchema, aTable).getOrDefault(aTable, new CaseInsensitiveMap<>(new LinkedHashMap<>()));
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
        Map<String, Map<String, PrimaryKey>> tablesPks = new CaseInsensitiveMap<>(new LinkedHashMap<>());
        Map<String, Map<String, ForeignKey>> tablesFks = new CaseInsensitiveMap<>(new LinkedHashMap<>());
        Map<String, Map<String, JdbcColumn>> tablesColumns = new CaseInsensitiveMap<>(new LinkedHashMap<>());
        if (r != null) {
            Map<String, Integer> colIndices = columnsIndices(r.getMetaData());
            int colTableName = colIndices.get(JDBC_TABLE_NAME);
            int colColumnName = colIndices.get(JDBC_COLUMN_NAME);
            int colRemarks = colIndices.get(JDBC_REMARKS);
            int colDataType = colIndices.get(JDBC_DATA_TYPE);
            int colTypeName = colIndices.get(JDBC_TYPE_NAME);
            int colColumnSize = colIndices.get(JDBC_COLUMN_SIZE);
            int colDecimalDigits = colIndices.get(JDBC_DECIMAL_DIGITS);
            int colNumPrecRadix = colIndices.get(JDBC_NUMBER_PRECISION_RADIX);
            int colNullable = colIndices.get(JDBC_NULLABLE);
            while (r.next()) {
                String fTableName = r.getString(colTableName);
                Map<String, JdbcColumn> columns = tablesColumns.computeIfAbsent(fTableName, tn -> new CaseInsensitiveMap<>(new LinkedHashMap<>()));
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
                        return queryTablePrimaryKeys(meta, schema, fTableName);
                    } catch (SQLException ex) {
                        throw new UncheckedSQLException(ex);
                    }
                });
                Map<String, ForeignKey> fks = tablesFks.computeIfAbsent(fTableName, tn -> {
                    try {
                        return queryTableForeignKeys(meta, schema, fTableName);
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
            return readTablesPrimaryKeys(r).getOrDefault(table, new CaseInsensitiveMap<>(new LinkedHashMap<>()));
        }
    }

    private static Map<String, ForeignKey> queryTableForeignKeys(DatabaseMetaData meta, String schema, String table) throws SQLException {
        try (ResultSet r = meta.getImportedKeys(null, schema, table)) {
            return readTablesForeignKeys(r).getOrDefault(table, new CaseInsensitiveMap<>(new LinkedHashMap<>()));
        }
    }

    private static Map<String, Map<String, PrimaryKey>> readTablesPrimaryKeys(ResultSet r) throws SQLException {
        Map<String, Map<String, PrimaryKey>> tablesKeys = new CaseInsensitiveMap<>(new LinkedHashMap<>());
        if (r != null) {
            Map<String, Integer> colsIndices = columnsIndices(r.getMetaData());
            int colTableSchema = colsIndices.get(JDBC_TABLE_SCHEMA);
            int colTableName = colsIndices.get(JDBC_TABLE_NAME);
            int colColumnName = colsIndices.get(JDBC_COLUMN_NAME);
            int colConstraintName = colsIndices.get(JDBC_PK_CONSTRAINT_NAME);
            while (r.next()) {
                String lpkSchema = r.getString(colTableSchema);
                String lpkTableName = r.getString(colTableName);
                String lpkField = r.getString(colColumnName);
                String lpkName = r.getString(colConstraintName);
                Map<String, PrimaryKey> tableKeys = tablesKeys.computeIfAbsent(lpkTableName, tn -> new CaseInsensitiveMap<>(new LinkedHashMap<>()));
                tableKeys.put(lpkField, new PrimaryKey(lpkSchema, lpkTableName, lpkField, lpkName));
            }
        }
        return tablesKeys;
    }

    private static Map<String, Map<String, ForeignKey>> readTablesForeignKeys(ResultSet r) throws SQLException {
        Map<String, Map<String, ForeignKey>> tablesKeys = new CaseInsensitiveMap<>(new LinkedHashMap<>());
        if (r != null) {
            Map<String, Integer> colsIndices = columnsIndices(r.getMetaData());
            int colFkTableSchema = colsIndices.get(JDBC_FK_TABLE_SCHEMA);
            int colFkTableName = colsIndices.get(JDBC_FK_TABLE_NAME);
            int colFkColumnName = colsIndices.get(JDBC_FK_COLUMN_NAME);
            int colFkName = colsIndices.get(JDBC_FK_CONSTRAINT_NAME);
            int colFkUpdateRule = colsIndices.get(JDBC_FK_UPDATE_RULE);
            int colFkDeleteRule = colsIndices.get(JDBC_FK_DELETE_RULE);
            int colFkDeferrability = colsIndices.get(JDBC_FK_DEFERRABILITY);
            //
            int colFkPkTableSchema = colsIndices.get(JDBC_FK_PK_TABLE_SCHEMA);
            int colFkPkTableName = colsIndices.get(JDBC_FK_PK_TABLE_NAME);
            int colFkPkColumnName = colsIndices.get(JDBC_FK_PK_COLUMN_NAME);
            int colFkPkName = colsIndices.get(JDBC_PK_CONSTRAINT_NAME);
            while (r.next()) {
                String lfkSchema = r.getString(colFkTableSchema);
                String lfkTableName = r.getString(colFkTableName);
                String lfkField = r.getString(colFkColumnName);
                String lfkName = r.getString(colFkName);
                short lfkUpdateRule = r.getShort(colFkUpdateRule);
                short lfkDeleteRule = r.getShort(colFkDeleteRule);
                short lfkDeferrability = r.getShort(colFkDeferrability);
                //
                String lpkSchema = r.getString(colFkPkTableSchema);
                String lpkTableName = r.getString(colFkPkTableName);
                String lpkField = r.getString(colFkPkColumnName);
                String lpkName = r.getString(colFkPkName);
                //
                Map<String, ForeignKey> tableKeys = tablesKeys.computeIfAbsent(lfkTableName, tn -> new CaseInsensitiveMap<>(new LinkedHashMap<>()));
                tableKeys.put(lfkField, new ForeignKey(lfkSchema, lfkTableName, lfkField, lfkName,
                        ForeignKey.ForeignKeyRule.valueOf(lfkUpdateRule), ForeignKey.ForeignKeyRule.valueOf(lfkDeleteRule), lfkDeferrability == 5,
                        lpkSchema, lpkTableName, lpkField, lpkName));
            }
        }
        return tablesKeys;
    }

    private static Map<String, Integer> columnsIndices(ResultSetMetaData metaData) throws SQLException {
        final Map<String, Integer> indices = new CaseInsensitiveMap<>(new HashMap<>());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String asName = metaData.getColumnLabel(i);
            String name = asName != null && !asName.isEmpty() ? asName : metaData.getColumnName(i);
            indices.put(name.toLowerCase(), i);
        }
        return indices;
    }
}
