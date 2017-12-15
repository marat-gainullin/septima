package com.septima.queries;

import com.septima.Database;
import com.septima.application.ApplicationDataProvider;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.changes.Command;
import com.septima.NamedValue;
import com.septima.dataflow.JdbcDataProvider;
import com.septima.metadata.Field;
import com.septima.Parameter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A compiled Sql query.
 * <p>
 * <p>An instance of this class contains JDBC-compliant SQL query text with "?"
 * placeholders for parameters and all parameters values.</p>
 *
 * @author mg
 */
public class SqlCompiledQuery {

    private final Database database;
    private final String entityName;
    private final String sqlClause;
    private final List<Parameter> parameters;
    private final boolean procedure;
    private final int pageSize;
    private final Map<String, Field> expectedFields;

    SqlCompiledQuery(Database aDatabase, String aEntityName, String aSqlClause, List<Parameter> aParams, boolean aProcedure, int aPageSize, Map<String, Field> aExpectedFields) {
        super();
        database = aDatabase;
        entityName = aEntityName;
        sqlClause = aSqlClause;
        parameters = aParams;
        procedure = aProcedure;
        pageSize = aPageSize;
        expectedFields = aExpectedFields;
    }

    public boolean isProcedure() {
        return procedure;
    }

    public int getPageSize() {
        return pageSize;
    }

    /**
     * Executes query and returns results regardless of procedure flag.
     * It uses query's own parameters.
     */
    public CompletableFuture<Collection<Map<String, Object>>> executeQuery() {
        return executeQuery(parameters);
    }

    /**
     * Executes query and returns results regardless of procedure flag.
     */
    public CompletableFuture<Collection<Map<String, Object>>> executeQuery(List<Parameter> aParameters) {
        Objects.requireNonNull(database);
        Objects.requireNonNull(aParameters);
        ApplicationDataProvider dataProvider = database.createDataProvider(entityName, sqlClause, procedure, pageSize, expectedFields);
        return dataProvider.pull(aParameters);
    }

    public Command prepareCommand() {
        return prepareCommand(parameters);
    }

    public Command prepareCommand(List<Parameter> aParameters) {
        Objects.requireNonNull(aParameters);
        return new Command(
                entityName,
                sqlClause,
                Collections.unmodifiableList(aParameters.stream()
                .map(parameter -> new NamedValue(parameter.getName(), parameter.getValue()))
                .collect(Collectors.toList())));
    }

    public CompletableFuture<Integer> executeUpdate() {
        CompletableFuture<Integer> updating = new CompletableFuture<>();
        database.getJdbcPerformer().accept(() -> {
            try {
                DataSource dataSource = database.getDataSource();
                try (Connection connection = dataSource.getConnection()) {
                    boolean autoCommit = connection.getAutoCommit();
                    connection.setAutoCommit(false);
                    try {
                        try (PreparedStatement stmt = connection.prepareStatement(sqlClause)) {
                            for (int i = 0; i < parameters.size(); i++) {
                                Parameter param = parameters.get(i);
                                int jdbcType = JdbcDataProvider.calcJdbcType(param.getType(), param.getValue());
                                JdbcDataProvider.assign(param.getValue(), i + 1, stmt, jdbcType, null);
                            }
                            try {
                                int rowsAffected = stmt.executeUpdate();
                                connection.commit();
                                updating.completeAsync(() -> rowsAffected, database.getFutureExecutor());
                            } catch (SQLException | UncheckedSQLException ex) {
                                connection.rollback();
                                throw ex;
                            }
                        }
                    } finally {
                        connection.setAutoCommit(autoCommit);
                    }
                }
            } catch (SQLException | UncheckedSQLException ex) {
                database.getFutureExecutor().execute(() -> {
                    updating.completeExceptionally(ex);
                });
            }
        });
        return updating;
    }

    /**
     * Returns the SQL query text.
     *
     * @return the SQL query text.
     */
    public String getSqlClause() {
        return sqlClause;
    }

    /**
     * Returns the vector of parameters' values.
     *
     * @return the vector of parameters' values.
     */
    public List<Parameter> getParameters() {
        return parameters;
    }

    public String getEntityName() {
        return entityName;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("Sql query: ");
        sb.append(sqlClause);
        if (!parameters.isEmpty()) {
            sb.append(" {");
            String delimiter = "";
            for (int i = 0; i < parameters.size(); i++) {
                Parameter param = parameters.get(i);
                sb.append(delimiter).append(Integer.toString(i + 1)).append('@').append(param.getValue() == null ? "null" : param.getValue().toString());
                delimiter = ", ";
            }
            sb.append("}");
        }
        return sb.toString();
    }

}
