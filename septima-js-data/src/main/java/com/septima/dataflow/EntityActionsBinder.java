package com.septima.dataflow;

import com.septima.metadata.Parameter;
import com.septima.changes.*;
import com.septima.entities.SqlEntity;
import com.septima.jdbc.JdbcReaderAssigner;
import com.septima.metadata.EntityField;
import com.septima.queries.SqlQuery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class EntityActionsBinder implements EntityActionsVisitor {

    /**
     * Short live information about sqlClause and its parameters.
     * Its like {@link SqlQuery}, but much simpler.
     * Performs parametrized prepared statements
     * execution.
     */
    public static class BoundStatement {

        private static final Logger QUERIES_LOGGER = Logger.getLogger(BoundStatement.class.getName());

        private final String clause;
        private final List<Parameter> parameters;
        private final JdbcReaderAssigner jdbcReaderAssigner;

        BoundStatement(String aClause, List<Parameter> aParameters, JdbcReaderAssigner aJdbcReaderAssigner) {
            super();
            clause = aClause;
            parameters = aParameters;
            jdbcReaderAssigner = aJdbcReaderAssigner;
        }

        public int apply(Connection aConnection) throws SQLException {
            try (PreparedStatement stmt = aConnection.prepareStatement(clause)) {
                for (int i = 0; i < parameters.size(); i++) {
                    jdbcReaderAssigner.assignInParameter(parameters.get(i), stmt, i + 1, aConnection);
                }
                if (QUERIES_LOGGER.isLoggable(Level.FINE)) {
                    QUERIES_LOGGER.log(Level.FINE, "Executing sql with {0} parameters: {1}", new Object[]{parameters.size(), clause});
                }
                return stmt.executeUpdate();
            }
        }
    }

    private static final String INSERT_CLAUSE = "insert into %s (%s) values (%s)";
    private static final String DELETE_CLAUSE = "delete from %s where %s";
    private static final String UPDATE_CLAUSE = "update %s set %s where %s";

    private final List<BoundStatement> logEntries = new ArrayList<>();
    private final SqlEntity entity;

    public EntityActionsBinder(SqlEntity aEntity) {
        super();
        entity = aEntity;
    }

    public List<BoundStatement> getLogEntries() {
        return logEntries;
    }

    private Function<Map.Entry<String, Object>, Map.Entry<String, Parameter>> asTableDatumEntry(SqlEntity aEntity) {
        return datum -> {
            String datumName = datum.getKey();
            Object datumValue = datum.getValue();
            EntityField entityEntityField = aEntity.getFields().get(datumName);
            if (entityEntityField != null) {
                String keyColumnName = entityEntityField.getOriginalName() != null ? entityEntityField.getOriginalName() : entityEntityField.getName();
                Parameter bound = new Parameter(keyColumnName, datumValue, entityEntityField.getType());
                return Map.entry(entityEntityField.getTableName(), bound);
            } else {
                throw new IllegalStateException("Entity field '" + datumName + "' is not found in entity '" + aEntity.getName() + "'");
            }
        };
    }

    @Override
    public void visit(InstanceAdd aAdd) {
        JdbcReaderAssigner jdbcReaderAssigner = entity.getDatabase().jdbcReaderAssigner(entity.isProcedure());
        aAdd.getData().entrySet().stream()
                .map(asTableDatumEntry(entity))
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet().stream()
                .filter(entry ->
                        !entry.getValue().isEmpty() &&
                                (entity.getWritable().isEmpty() || entity.getWritable().contains(entry.getKey()))
                )
                .map(entry -> new BoundStatement(
                        String.format(INSERT_CLAUSE,
                                entry.getKey(),
                                generateInsertColumnsClause(entry.getValue()),
                                generatePlaceholders(entry.getValue().size())
                        ),
                        Collections.unmodifiableList(entry.getValue()),
                        jdbcReaderAssigner
                ))
                .forEach(logEntries::add);
    }

    @Override
    public void visit(InstanceChange aUpdate) {
        JdbcReaderAssigner jdbcReaderAssigner = entity.getDatabase().jdbcReaderAssigner(entity.isProcedure());
        Map<String, List<Parameter>> updatesKeys = aUpdate.getKeys().entrySet().stream()
                .map(asTableDatumEntry(entity))
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
        aUpdate.getData().entrySet().stream()
                .map(asTableDatumEntry(entity))
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty() && !updatesKeys.getOrDefault(entry.getKey(), List.of()).isEmpty() &&
                        (entity.getWritable().isEmpty() || entity.getWritable().contains(entry.getKey()))
                )
                .map(entry -> new BoundStatement(
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
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet().stream()
                .filter(entry ->
                        !entry.getValue().isEmpty() &&
                                (entity.getWritable().isEmpty() || entity.getWritable().contains(entry.getKey()))
                )
                .map(entry -> new BoundStatement(
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
                query.getSqlClause(),
                Collections.unmodifiableList(query.getParameters().stream()
                        .map(queryParameter -> new Parameter(
                                queryParameter.getName(),
                                aCommand.getArguments().getOrDefault(queryParameter.getName(), queryParameter.getValue()),
                                queryParameter.getType(),
                                queryParameter.getMode(),
                                queryParameter.getDescription())
                        )
                        .collect(Collectors.toList())),
                jdbcReaderAssigner
        ));
    }

    private static String generatePlaceholders(int count) {
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

    private static String generateUpdateColumnsClause(List<Parameter> data) {
        return data.stream()
                .map(datum -> new StringBuilder(datum.getName()).append(" = ?"))
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
}
