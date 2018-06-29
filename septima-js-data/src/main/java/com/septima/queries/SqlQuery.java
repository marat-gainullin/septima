package com.septima.queries;

import com.septima.Database;
import com.septima.GenericType;
import com.septima.dataflow.DynamicTypingDataProvider;
import com.septima.jdbc.JdbcReaderAssigner;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import com.septima.metadata.Parameter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public class SqlQuery {

    private final Database database;
    private final String entityName;
    private final String sqlClause;
    private final List<Parameter> parameters;
    private final boolean procedure;
    private final int pageSize;
    private final Map<String, EntityField> expectedFields;

    public SqlQuery(Database aDatabase, String aEntityName, String aSqlClause, List<Parameter> aParams, boolean aProcedure, int aPageSize, Map<String, EntityField> aExpectedFields) {
        super();
        database = aDatabase;
        entityName = aEntityName;
        sqlClause = aSqlClause;
        parameters = aParams;
        procedure = aProcedure;
        pageSize = aPageSize;
        expectedFields = aExpectedFields;
    }

    public Database getDatabase() {
        return database;
    }

    public boolean isProcedure() {
        return procedure;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Map<String, EntityField> getExpectedFields() {
        return expectedFields;
    }

    private List<Parameter> mergeParametersValues(Map<String, Object> aParametersValues) {
        return parameters.stream()
                .map(own -> new Parameter(
                        own.getName(),
                        aParametersValues.getOrDefault(own.getName(), own.getValue()),
                        own.getType(),
                        own.getMode(),
                        own.getDescription())
                )
                .collect(Collectors.toList());
    }

    /**
     * Executes query and returns results future.
     * It uses query's own parameters.
     *
     * @return {@link CompletableFuture} The future of requested data.
     */
    public CompletableFuture<List<Map<String, Object>>> requestData() {
        return requestData(Map.of());
    }

    /**
     * Executes query and returns results future.
     * It uses its parameters as is and gets parameters' values form {@code aParametersValues} argument.
     *
     * @param aParametersValues Used as parameters' values source. If some parameter's value is not found is this map,
     *                          value of own parameter is used as the default.
     * @return {@link CompletableFuture} The future of requested data.
     */
    public CompletableFuture<List<Map<String, Object>>> requestData(Map<String, Object> aParametersValues) {
        Objects.requireNonNull(aParametersValues, "aParametersValues is required argument");
        Objects.requireNonNull(database);
        DynamicTypingDataProvider dataProvider = database.createDataProvider(entityName, sqlClause, procedure, pageSize, expectedFields);
        return dataProvider.pull(mergeParametersValues(aParametersValues));
    }

    public Map<String, Object> parseParameters(Map<String, String> aParametersValues) {
        Objects.requireNonNull(aParametersValues, "aParametersValues is required argument");
        Map<String, GenericType> types = parameters.stream()
                .collect(Collectors.toMap(Parameter::getName, p -> p.getType() != null ? p.getType() : GenericType.STRING, (v1, v2) -> v1));
        // Warning. Don't refactor to another .collect(Collectors.toMap( because of null values of parameters !
        Map<String, Object> values = new HashMap<>();
        for (Map.Entry<String, GenericType> e : types.entrySet()) {
            values.put(e.getKey(), GenericType.parseValue(aParametersValues.get(e.getKey()), e.getValue()));
        }
        return values;
    }

    /**
     * Executes query with default parameters' values and returns affected rows count future.
     *
     * @return {@link CompletableFuture} The future of affected rows count.
     */
    public CompletableFuture<Integer> start() {
        return start(Map.of());
    }

    /**
     * Executes query and returns affected rows count future.
     * It uses its parameters as is and gets parameters' values form {@code aParametersValues} argument.
     *
     * @param aParametersValues Used as parameters' values source. If some parameter's value is not found is this map,
     *                          value of own parameter is used as the default.
     * @return {@link CompletableFuture} The future of affected rows count.
     */
    public CompletableFuture<Integer> start(Map<String, Object> aParametersValues) {
        Objects.requireNonNull(aParametersValues, "aParametersValues is required argument");
        List<Parameter> linearParameters = mergeParametersValues(aParametersValues);
        CompletableFuture<Integer> updating = new CompletableFuture<>();
        database.getJdbcPerformer().execute(() -> {
            try {
                JdbcReaderAssigner jdbcReaderAssigner = database.jdbcReaderAssigner(procedure);
                DataSource dataSource = database.getDataSource();
                try (Connection connection = dataSource.getConnection()) {
                    boolean autoCommit = connection.getAutoCommit();
                    connection.setAutoCommit(false);
                    try {
                        try (PreparedStatement stmt = connection.prepareStatement(sqlClause)) {
                            for (int i = 0; i < linearParameters.size(); i++) {
                                jdbcReaderAssigner.assignInParameter(linearParameters.get(i), stmt, i + 1, connection);
                            }
                            try {
                                int rowsAffected = stmt.executeUpdate();
                                connection.commit();
                                updating.completeAsync(() -> rowsAffected, database.getFuturesExecutor());
                            } catch (SQLException | UncheckedSQLException ex) {
                                connection.rollback();
                                throw ex;
                            }
                        }
                    } finally {
                        connection.setAutoCommit(autoCommit);
                    }
                }
            } catch (Throwable ex) {
                database.getFuturesExecutor().execute(() -> updating.completeExceptionally(ex));
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
