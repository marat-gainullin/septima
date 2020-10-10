package com.septima.dataflow;

import com.septima.changes.*;
import com.septima.entities.SqlEntity;
import com.septima.jdbc.JdbcReaderAssigner;
import com.septima.metadata.EntityField;
import com.septima.metadata.Parameter;
import com.septima.queries.SqlQuery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class EntityActionsBinder implements EntityActionsVisitor {

    private static final String INSERT_CLAUSE = "insert into %s (%s) values (%s)";
    private static final String DELETE_CLAUSE = "delete from %s where %s";
    private static final String UPDATE_CLAUSE = "update %s set %s where %s";
    private final List<BoundStatement> logEntries = new ArrayList<>();
    private final SqlEntity entity;

    public EntityActionsBinder(SqlEntity aEntity) {
        super();
        entity = aEntity;
    }

    private String generatePlaceholders(List<Parameter> parameters) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parameters.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(entity.getDatabase().getSqlDriver().parameterPlaceholder(parameters.get(i)));
        }
        return sb.toString();
    }

    private String generateUpdateColumnsClause(List<Parameter> parameters) {
        return parameters.stream()
                .map(parameter -> new StringBuilder(parameter.getName()).append(" = " + entity.getDatabase().getSqlDriver().parameterPlaceholder(parameter)))
                .reduce((name1, name2) -> name1.append(", ").append(name2))
                .map(StringBuilder::toString)
                .orElse("");
    }

    private static String generateInsertColumnsClause(List<Parameter> data) {
        return data.stream()
                .map(datum -> new StringBuilder(datum.getName()))
                .reduce((name1, name2) -> name1.append(", ").append(name2))
                .map(StringBuilder::toString)
                .orElse("");
    }

    private static <E> List<E> concat(List<E> first, List<E> second) {
        List<E> general = new ArrayList<>(first.size() + second.size());
        general.addAll(first);
        general.addAll(second);
        return general;
    }

    public List<BoundStatement> getLogEntries() {
        return logEntries;
    }

    private Function<Map.Entry<String, Object>, Map.Entry<String, Parameter>> asTableDatumEntry(SqlEntity anEntity) {
        return datum -> {
            String datumName = datum.getKey();
            Object datumValue = datum.getValue();
            EntityField entityEntityField = anEntity.getFields().get(datumName);
            if (entityEntityField != null) {
                String keyColumnName = entityEntityField.getOriginalName() != null ? entityEntityField.getOriginalName() : entityEntityField.getName();
                Parameter bound = new Parameter(keyColumnName, datumValue, entityEntityField.getType(), entityEntityField.getSubType(), Parameter.Mode.In, null);
                if (entityEntityField.getTableName() != null) {
                    return Map.entry(entityEntityField.getTableName(), bound);
                } else {
                    throw new IllegalStateException("Entity field '" + datumName + "' of entity '" + anEntity.getName() + "' has no source table");
                }
            } else {
                throw new IllegalStateException("Entity field '" + datumName + "' is not found in entity '" + anEntity.getName() + "'");
            }
        };
    }

    @Override
    public void visit(InstanceAdd aAdd) {
        JdbcReaderAssigner jdbcReaderAssigner = entity.getDatabase().jdbcReaderAssigner(entity.isProcedure());
        aAdd.getData().entrySet().stream()
                .map(asTableDatumEntry(entity))
                .collect(Collectors.groupingBy(Map.Entry::getKey, LinkedHashMap::new,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet().stream()
                .filter(entry ->
                        !entry.getValue().isEmpty() &&
                                (entity.getWritable().isEmpty() || entity.getWritable().contains(entry.getKey()))
                )
                .map(entry -> new BoundStatement(
                        aAdd.getEntityName(),
                        String.format(INSERT_CLAUSE,
                                entry.getKey(),
                                generateInsertColumnsClause(entry.getValue()),
                                generatePlaceholders(entry.getValue())
                        ),
                        Collections.unmodifiableList(entry.getValue()),
                        jdbcReaderAssigner
                ))
                .forEach(logEntries::add);
    }

    @Override
    public void visit(InstanceChange anUpdate) {
        JdbcReaderAssigner jdbcReaderAssigner = entity.getDatabase().jdbcReaderAssigner(entity.isProcedure());
        Map<String, List<Parameter>> updatesKeys = anUpdate.getKeys().entrySet().stream()
                .map(asTableDatumEntry(entity))
                .collect(Collectors.groupingBy(Map.Entry::getKey, LinkedHashMap::new,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
        anUpdate.getData().entrySet().stream()
                .map(asTableDatumEntry(entity))
                .collect(Collectors.groupingBy(Map.Entry::getKey, LinkedHashMap::new,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty() && !updatesKeys.getOrDefault(entry.getKey(), List.of()).isEmpty() &&
                        (entity.getWritable().isEmpty() || entity.getWritable().contains(entry.getKey()))
                )
                .map(entry -> new BoundStatement(
                        anUpdate.getEntityName(),
                        String.format(UPDATE_CLAUSE,
                                entry.getKey(),
                                generateUpdateColumnsClause(entry.getValue()),
                                generateWhereClause(updatesKeys.getOrDefault(entry.getKey(), List.of()))
                        ),
                        Collections.unmodifiableList(concat(entry.getValue(), updatesKeys.getOrDefault(entry.getKey(), List.of()))),
                        jdbcReaderAssigner
                ))
                .forEach(logEntries::add);
    }

    /**
     * Generates deletion statements for an entity for all underlying tables.
     * It can reorder deletion statements for tables.
     * So, you should avoid deletion from compound entities interlinked with foreign keys.
     * In general, you shouldn't meet such case, because interlinked tables should interact via
     * foreign keys rather than via this multi tables deletion.
     *
     * @param aRemove Deletion command transform delete from all underlying tables indices an entity
     */
    @Override
    public void visit(InstanceRemove aRemove) {
        JdbcReaderAssigner jdbcReaderAssigner = entity.getDatabase().jdbcReaderAssigner(entity.isProcedure());
        aRemove.getKeys().entrySet().stream()
                .map(asTableDatumEntry(entity))
                .collect(Collectors.groupingBy(Map.Entry::getKey, LinkedHashMap::new,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet().stream()
                .filter(entry ->
                        !entry.getValue().isEmpty() &&
                                (entity.getWritable().isEmpty() || entity.getWritable().contains(entry.getKey()))
                )
                .map(entry -> new BoundStatement(
                        aRemove.getEntityName(),
                        String.format(DELETE_CLAUSE,
                                entry.getKey(),
                                generateWhereClause(entry.getValue())
                        ),
                        Collections.unmodifiableList(entry.getValue()),
                        jdbcReaderAssigner
                ))
                .forEach(logEntries::add);
    }

    @Override
    public void visit(EntityCommand aCommand) {
        JdbcReaderAssigner jdbcReaderAssigner = entity.getDatabase().jdbcReaderAssigner(entity.isProcedure());
        SqlQuery query = entity.toQuery();
        logEntries.add(new BoundStatement(
                aCommand.getEntityName(),
                query.getSqlClause(),
                Collections.unmodifiableList(query.getParameters().stream()
                        .map(queryParameter -> new Parameter(
                                queryParameter.getName(),
                                aCommand.getArguments().getOrDefault(queryParameter.getName(), queryParameter.getValue()),
                                queryParameter.getType(),
                                queryParameter.getSubType(),
                                queryParameter.getMode(),
                                queryParameter.getDescription())
                        )
                        .collect(Collectors.toList())),
                jdbcReaderAssigner
        ));
    }

    /**
     * Generates sql where clause string for values array passed in. It's assumed
     * that key columns may not have NULL values. This assumption is made
     * because we use simple "=" operator in WHERE clause.
     *
     * @param keys Keys array transform deal with.
     * @return Generated Where clause.
     */
    private String generateWhereClause(List<Parameter> keys) {
        return keys.stream()
                .map(datum -> new StringBuilder(datum.getName()).append(" = ?"))
                .reduce((name1, name2) -> name1.append(" and ").append(name2))
                .map(StringBuilder::toString)
                .orElse("");
    }

    /**
     * Short live information about sqlClause and its parameters.
     * Its like {@link SqlQuery}, but much simpler.
     * Performs parametrized prepared statements
     * execution.
     */
    public static class BoundStatement {

        private static final Logger QUERIES_LOGGER = Logger.getLogger(BoundStatement.class.getName());

        private final String entityName;
        private final String clause;
        private final List<Parameter> parameters;
        private final JdbcReaderAssigner jdbcReaderAssigner;

        BoundStatement(String aEntityName, String aClause, List<Parameter> aParameters, JdbcReaderAssigner aJdbcReaderAssigner) {
            super();
            entityName = aEntityName;
            clause = aClause;
            parameters = aParameters;
            jdbcReaderAssigner = aJdbcReaderAssigner;
        }

        public String getEntityName() {
            return entityName;
        }

        public String getClause() {
            return clause;
        }

        public int apply(Connection aConnection) throws SQLException {
            try (PreparedStatement stmt = aConnection.prepareStatement(clause)) {
                assignParameters(aConnection, stmt);
                return stmt.executeUpdate();
            }
        }

        public void assignParameters(Connection aConnection, PreparedStatement stmt) throws SQLException {
            if (QUERIES_LOGGER.isLoggable(Level.FINE)) {
                QUERIES_LOGGER.log(Level.FINE, "About to execute sql with {0} parameters: {1}", new Object[]{parameters.size(), clause});
            }
            for (int i = 0; i < parameters.size(); i++) {
                jdbcReaderAssigner.assignInParameter(parameters.get(i), stmt, i + 1, aConnection);
            }
        }
    }
}
