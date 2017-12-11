package com.septima.dataflow;

import com.septima.Constants;
import com.septima.changes.*;
import com.septima.changes.NamedValue;
import com.septima.EntitiesHost;
import com.septima.metadata.Field;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.Parameter;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Writer for jdbc datasources. Performs writing of a rowset. Writing utilizes
 * converters to produce jdbc-specific data while writing. There are two modes
 * of database updating. The first one "write mode" is update/delete/insert
 * statements preparation and batch execution. The second one "log mode" is
 * logging of statements to be executed with parameters values. In log mode no
 * execution is performed.
 *
 * @author mg
 */
public class StatementsGenerator implements ApplicableChangeVisitor {

    public interface TablesContainer {

        Optional<Map<String, JdbcColumn>> getTable(String aTableName) throws Exception;
    }

    public interface GeometryConverter {

        NamedJdbcValue convertGeometry(String aValue, Connection aConnection) throws SQLException;
    }

    /**
     * Stores short living information about statements, to be executed while
     * jdbc update process. Performs parameterized prepared statements
     * execution.
     */
    public static class GeneratedStatement {

        private static final Logger QUERIES_LOGGER = Logger.getLogger(GeneratedStatement.class.getName());
        private final String clause;
        private final List<NamedValue> parameters;
        private final boolean valid; // TODO: Think about how to remove this flag
        private final GeometryConverter geometryConverter;

        GeneratedStatement(String aClause, List<NamedValue> aParameters, boolean aValid, GeometryConverter aGeometryConverter) {
            super();
            clause = aClause;
            parameters = aParameters;
            valid = aValid;
            geometryConverter = aGeometryConverter;
        }

        public int apply(Connection aConnection) throws Exception {
            if (valid) {
                try (PreparedStatement stmt = aConnection.prepareStatement(clause)) {
                    ParameterMetaData pMeta = stmt.getParameterMetaData();
                    for (int i = 1; i <= parameters.size(); i++) {
                        NamedValue v = parameters.get(i - 1);
                        Object value;
                        int jdbcType;
                        String sqlTypeName;
                        if (v instanceof NamedJdbcValue) {
                            NamedJdbcValue jv = (NamedJdbcValue) v;
                            value = jv.getValue();
                            jdbcType = jv.getJdbcType();
                            sqlTypeName = jv.getSqlTypeName();
                        } else if (v instanceof NamedGeometryValue) {
                            NamedJdbcValue jv = geometryConverter.convertGeometry(v.getValue() != null ? v.getValue().toString() : null, aConnection);
                            value = jv.getValue();
                            jdbcType = jv.getJdbcType();
                            sqlTypeName = jv.getSqlTypeName();
                        } else {
                            value = v.getValue();
                            try {
                                jdbcType = pMeta.getParameterType(i);
                                sqlTypeName = pMeta.getParameterTypeName(i);
                            } catch (SQLException ex) {
                                Logger.getLogger(StatementsGenerator.class.getName()).log(Level.WARNING, null, ex);
                                jdbcType = JdbcDataProvider.assumeJdbcType(v.getValue());
                                sqlTypeName = null;
                            }
                        }
                        JdbcDataProvider.assign(value, i, stmt, jdbcType, sqlTypeName);
                    }
                    if (QUERIES_LOGGER.isLoggable(Level.FINE)) {
                        QUERIES_LOGGER.log(Level.FINE, "Executing sql with {0} parameters: {1}", new Object[]{parameters.size(), clause});
                    }
                    return stmt.executeUpdate();
                }
            } else {
                Logger.getLogger(GeneratedStatement.class.getName()).log(Level.INFO, "Invalid GeneratedStatement occured!");
                return 0;
            }
        }
    }

    private static final String INSERT_CLAUSE = "insert into %s (%s) values (%s)";
    private static final String DELETE_CLAUSE = "delete from %s where %s";
    private static final String UPDATE_CLAUSE = "update %s set %s where %s";
    private List<GeneratedStatement> logEntries = new ArrayList<>();
    private EntitiesHost entitiesHost;
    private TablesContainer tables;
    private GeometryConverter geometryConverter;

    public StatementsGenerator(EntitiesHost aEntitiesHost, TablesContainer aTables, GeometryConverter aGeometryConverter) {
        super();
        entitiesHost = aEntitiesHost;
        tables = aTables;
        geometryConverter = aGeometryConverter;
    }

    public List<GeneratedStatement> getLogEntries() {
        return logEntries;
    }

    private NamedValue bindNamedValueToTable(final String aTableName, final String aColumnName, final Object aValue) throws Exception {
        return tables.getTable(aTableName)
                .map(tableFields -> tableFields.get(aColumnName))
                .map(tableField -> (NamedValue) new NamedJdbcValue(aColumnName, aValue, tableField.getJdbcType(), tableField.getType()))
                .orElseGet(() -> new NamedValue(aColumnName, aValue));
    }

    private String generatePlaceholders(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("?");
        }
        return sb.toString();
    }

    /**
     * Generates sql where clause string for values array passed in. It's assumed
     * that key columns may not have NULL values. This assumption is made
     * because we use simple "=" operator in WHERE clause.
     *
     * @param aKeys Keys array to deal with.
     * @return Generated Where clause.
     */
    private String generateWhereClause(List<NamedValue> aKeys) {
        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < aKeys.size(); i++) {
            if (i > 0) {
                whereClause.append(" and ");
            }
            whereClause.append(aKeys.get(i).getName()).append(" = ?");
        }
        return whereClause.toString();
    }

    private static class InsertInto {

        private final String table;
        private final List<NamedValue> data;

        InsertInto(String aTable, List<NamedValue> aData) {
            table = aTable;
            data = aData;
        }

        public String getTable() {
            return table;
        }

        public List<NamedValue> getData() {
            return data;
        }
    }

    @Override
    public void visit(Insert aChange) throws Exception {
        aChange.getData().stream()
                .map(datum -> {
                    try {
                        Field entityField = entitiesHost.resolveField(aChange.getEntity(), datum.getName());
                        String dataColumnName = entityField.getOriginalName() != null ? entityField.getOriginalName() : entityField.getName();
                        NamedValue bound = Constants.GEOMETRY_TYPE_NAME.equals(entityField.getType()) ? new NamedGeometryValue(dataColumnName, datum.getValue()) : bindNamedValueToTable(entityField.getTableName(), dataColumnName, datum.getValue());
                        return new InsertInto(entityField.getTableName(), List.of(bound));
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                })
                .collect(Collectors.toMap(
                        insertInto -> insertInto.getTable(),
                        insertInto -> insertInto.getData(),
                        (data1, data2) -> {
                            data1.addAll(data2);
                            return data1;
                        }))
                .entrySet().stream()
                .map(e -> new InsertInto(e.getKey(), e.getValue()))
                .filter(insertInto -> !insertInto.getData().isEmpty())
                .map(insertInto -> new GeneratedStatement(
                        String.format(INSERT_CLAUSE, insertInto.getTable(), insertInto.getData().stream()
                                        .map(datum -> new StringBuilder(datum.getName()))
                                        .reduce((s1, s2) -> s1.append(", ").append(s2))
                                        .map(sb -> sb.toString())
                                        .orElse(""),
                                generatePlaceholders(insertInto.getData().size())
                        ),
                        insertInto.getData(),
                        true,
                        geometryConverter
                ))
                .forEach(logEntries::add);
        /*
        Map<String, InsertChunk> inserts = new HashMap<>();
        for (NamedValue datum : aChange.getData()) {
            Field field = entitiesHost.resolveField(aChange.getEntity(), datum.getName());
            if (field != null) {
                InsertChunk chunk = inserts.get(field.getTableName());
                if (chunk == null) {
                    chunk = new InsertChunk();
                    inserts.put(field.getTableName(), chunk);
                    chunk.insert = new GeneratedStatement(geometryConverter);
                    // Adding here is strongly needed. Because of order in which other and this statements are added
                    // to the log and therefore applied into a database during a transaction.
                    logEntries.add(chunk.insert);
                    chunk.dataColumnsNames = new StringBuilder();
                    chunk.keysColumnsNames = new ArrayList<>();
                }
                if (!chunk.insert.parameters.isEmpty()) {
                    chunk.dataColumnsNames.append(", ");
                }
                String dataColumnName = field.getOriginalName() != null ? field.getOriginalName() : field.getName();
                chunk.dataColumnsNames.append(dataColumnName);
                //
                NamedValue checked = Constants.GEOMETRY_TYPE_NAME.equals(field.getType()) ? new NamedGeometryValue(dataColumnName, datum.getValue()) : bindNamedValueToTable(field.getTableName(), dataColumnName, datum.getValue());
                chunk.insert.parameters.add(checked);
                if (field.isPk()) {
                    chunk.keysColumnsNames.add(dataColumnName);
                }
            }
        }
        for (String tableName : inserts.keySet()) {
            InsertChunk chunk = inserts.get(tableName);
            chunk.insert.setClause(String.format(INSERT_CLAUSE, tableName, chunk.dataColumnsNames.toString(), generatePlaceholders(chunk.insert.parameters.size())));
            // Validness of the insert statement is outlined by inserted columns and also by key columns existance
            // because we have to prevent unexpected inserts in any joined table.
            // In this case inserts will be valid only if they include at least one key column per table.
            // Another case is single table per Insert instance.
            // So, we can avoid unexpected inserts in a transaction.
            // It's considered that key-less inserts are easy to obtain with manual command queries.
            // So, avoid values in select columns list for a table to avoid unexpected inserts in that table.
            chunk.insert.valid = !chunk.insert.parameters.isEmpty() && (!chunk.keysColumnsNames.isEmpty() || inserts.size() == 1);
        }
        */
    }

    private class UpdateChunk {

        private GeneratedStatement update;
        private StringBuilder columnsClause;
        private List<NamedValue> keys;
        private List<NamedValue> data;
    }

    @Override
    public void visit(Update aChange) throws Exception {
        Map<String, UpdateChunk> updates = new HashMap<>();
        // data
        for (NamedValue datum : aChange.getData()) {
            Field field = entitiesHost.resolveField(aChange.getEntity(), datum.getName());
            if (field != null) {
                UpdateChunk chunk = updates.get(field.getTableName());
                if (chunk == null) {
                    chunk = new UpdateChunk();
                    updates.put(field.getTableName(), chunk);
                    chunk.update = new GeneratedStatement(geometryConverter);
                    // Adding here is strongly needed. Because of order in with other and this statements are added
                    // to the log and therefore applied into a database during a transaction.
                    logEntries.add(chunk.update);
                    chunk.columnsClause = new StringBuilder();
                    chunk.data = new ArrayList<>();
                }
                if (!chunk.data.isEmpty()) {
                    chunk.columnsClause.append(", ");
                }
                String dataColumnName = field.getOriginalName() != null ? field.getOriginalName() : field.getName();
                chunk.columnsClause.append(dataColumnName).append(" = ?");
                NamedValue checked = Constants.GEOMETRY_TYPE_NAME.equals(field.getType()) ? new NamedGeometryValue(dataColumnName, datum.value) : bindNamedValueToTable(field.getTableName(), dataColumnName, datum.value);
                chunk.data.add(checked);
            }
        }
        // keys
        for (NamedValue key : aChange.getKeys()) {
            Field field = entitiesHost.resolveField(aChange.getEntity(), key.getName());
            if (field != null) {
                UpdateChunk chunk = updates.get(field.getTableName());
                if (chunk != null) {
                    if (chunk.keys == null) {
                        chunk.keys = new ArrayList<>();
                    }
                    String keyColumnName = field.getOriginalName() != null ? field.getOriginalName() : field.getName();
                    NamedValue checked = Constants.GEOMETRY_TYPE_NAME.equals(field.getType()) ? new NamedGeometryValue(keyColumnName, key.value) : bindNamedValueToTable(field.getTableName(), keyColumnName, key.value);
                    chunk.keys.add(checked);
                }
            }
        }
        updates.entrySet().stream().forEach((Map.Entry<String, UpdateChunk> entry) -> {
            String tableName = entry.getKey();
            UpdateChunk chunk = entry.getValue();
            if (chunk.data != null && !chunk.data.isEmpty()
                    && chunk.keys != null && !chunk.keys.isEmpty()) {
                chunk.update.setClause(String.format(UPDATE_CLAUSE, tableName, chunk.columnsClause.toString(), generateWhereClause(chunk.keys)));
                chunk.update.parameters.addAll(chunk.data);
                chunk.update.parameters.addAll(chunk.keys);
                chunk.update.valid = true;
            } else {
                chunk.update.valid = false;
            }
        });
    }

    private static class DeleteFrom {
        private final String table;
        private final List<NamedValue> values;

        DeleteFrom(String aTable, List<NamedValue> aValues) {
            table = aTable;
            values = aValues;
        }

        public String getTable() {
            return table;
        }

        public List<NamedValue> getValues() {
            return values;
        }
    }

    /**
     * Generates deletion statements for an entity for all underlying tables.
     * It can reorder deletion statements for tables.
     * So, you should avoid deletion from compound entities interlinked with foreign keys.
     * In general, you shouldn't meet such case, because interlinked tables should interact via
     * foreign keys rather than via this multi tables deletion.
     *
     * @param aDeletion Deletion command to delete from all underlying tables of an entity
     * @throws Exception If problems occur while generating statements for deletion.
     */
    @Override
    public void visit(Delete aDeletion) throws Exception {
        aDeletion.getKeys().stream()
                .map(deletionKey -> {
                    try {
                        Field entityField = entitiesHost.resolveField(aDeletion.getEntity(), deletionKey.getName());
                        String keyColumnName = entityField.getOriginalName() != null ? entityField.getOriginalName() : entityField.getName();
                        NamedValue bound = Constants.GEOMETRY_TYPE_NAME.equals(entityField.getType()) ?
                                new NamedGeometryValue(keyColumnName, deletionKey.getValue()) :
                                bindNamedValueToTable(entityField.getTableName(), keyColumnName, deletionKey.getValue());
                        return new DeleteFrom(entityField.getTableName(), List.of(bound));
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                })
                .collect(Collectors.toMap(
                        deletion -> deletion.getTable(),
                        deletion -> deletion.getValues(),
                        (values1, values2) -> {
                            values1.addAll(values2);
                            return values1;
                        }))
                .entrySet().stream()
                .map(e -> new DeleteFrom(e.getKey(), e.getValue()))
                .filter(deletion -> !deletion.getValues().isEmpty())
                .map(deletion -> new GeneratedStatement(
                        String.format(DELETE_CLAUSE, deletion.getTable(), generateWhereClause(deletion.getValues())),
                        deletion.getValues(),
                        true,
                        geometryConverter
                ))
                .forEach(logEntries::add);
        /*
        Map<String, GeneratedStatement> deletes = new HashMap<>();
        for (NamedValue key : aChange.getValues()) {
            Field field = entitiesHost.resolveField(aChange.getEntity(), key.getName());
            if (field != null) {
                GeneratedStatement delete = deletes.get(field.getTableName());
                if (delete == null) {
                    delete = new GeneratedStatement(geometryConverter);
                    deletes.put(field.getTableName(), delete);
                    // Adding here is strongly needed. Because of order in which other and this statements are added
                    // to the log and therefore applied into a database during a transaction.
                    logEntries.add(delete);
                }
                String keyColumnName = field.getOriginalName() != null ? field.getOriginalName() : field.getName();
                NamedValue checked = Constants.GEOMETRY_TYPE_NAME.equals(field.getType()) ? new NamedGeometryValue(keyColumnName, key.getValue()) : bindNamedValueToTable(field.getTableName(), keyColumnName, key.getValue());
                delete.parameters.add(checked);
            }
        }
        deletes.entrySet().stream().forEach((Map.Entry<String, GeneratedStatement> entry) -> {
            String tableName = entry.getKey();
            GeneratedStatement delete = entry.getValue();
            delete.setClause(String.format(DELETE_CLAUSE, tableName, generateWhereClause(delete.parameters)));
            delete.valid = !delete.parameters.isEmpty();
        });
        */
    }

    private static class NamedGeometryValue extends NamedValue {

        NamedGeometryValue(String aName, Object aValue) {
            super(aName, aValue);
        }
    }

    @Override
    public void visit(Command aChange) throws Exception {
        logEntries.add(new GeneratedStatement(
                aChange.getCommand(),
                Collections.unmodifiableList(aChange.getParameters().stream()
                        .map(cv -> {
                            try {
                                Parameter p = entitiesHost.resolveParameter(aChange.getEntity(), cv.getName());
                                if (cv.getValue() != null && Constants.GEOMETRY_TYPE_NAME.equals(p.getType())) {
                                    return new NamedGeometryValue(cv.getName(), cv.getValue());
                                } else {
                                    return new NamedJdbcValue(cv.getName(), cv.getValue(), JdbcDataProvider.calcJdbcType(p.getType(), cv.getValue()), null);
                                }
                            } catch (Exception ex) {
                                throw new IllegalStateException(ex);
                            }
                        })
                        .collect(Collectors.toList())),
                true,
                geometryConverter
        ));
    }
}
