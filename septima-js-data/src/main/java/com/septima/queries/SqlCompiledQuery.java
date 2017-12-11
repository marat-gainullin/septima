package com.septima.queries;

import com.septima.Database;
import com.septima.SeptimaDataProvider;
import com.septima.changes.Command;
import com.septima.changes.NamedValue;
import com.septima.dataflow.DataProvider;
import com.septima.dataflow.JdbcDataProvider;
import com.septima.metadata.Field;
import com.septima.metadata.Parameter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * A compiled SQL query.
 * <p>
 * <p>An instance of this class contains JDBC-compliant SQL query text with "?"
 * placeholders for parameters and all parameters values.</p>
 *
 * @author pk, mg
 */
public class SqlCompiledQuery {

    private final Database database;
    private final String entityName;
    private final String sqlClause;
    private final List<Parameter> parameters;
    private final Map<String, Field> expectedFields;
    private final boolean procedure;
    private final int pageSize;

    SqlCompiledQuery(Database aDatabase, String aSqlClause) {
        this(aDatabase, aSqlClause, List.of());
    }

    SqlCompiledQuery(Database aDatabase, String aSqlClause, List<Parameter> aParams) {
        this(aDatabase, aSqlClause, aParams, Map.of());
    }

    SqlCompiledQuery(Database aDatabase, String aSqlClause, List<Parameter> aParams, Map<String, Field> aExpectedFields) {
        this(aDatabase, null, aSqlClause, aParams, aExpectedFields, DataProvider.NO_PAGING_PAGE_SIZE, false);
    }

    SqlCompiledQuery(Database aDatabase, String aEntityName, String aSqlClause, List<Parameter> aParams, Map<String, Field> aExpectedFields, int aPageSize, boolean aProcedure) {
        super();
        sqlClause = aSqlClause;
        parameters = aParams;
        entityName = aEntityName;
        expectedFields = aExpectedFields;
        database = aDatabase;
        pageSize = aPageSize;
        procedure = aProcedure;
    }

    public boolean isProcedure() {
        return procedure;
    }

    public int getPageSize() {
        return pageSize;
    }

    /**
     * Executes query and returns results regardless of procedure flag.
     */
    public <T> T executeQuery(JdbcDataProvider.ResultSetProcessor<T> aResultSetProcessor, Executor aCallbacksExecutor, Consumer<T> onSuccess, Consumer<Exception> onFailure) throws Exception {
        if (database != null) {
            SeptimaDataProvider flow = database.createDataProvider(entityName, sqlClause, expectedFields);
            flow.setPageSize(pageSize);
            flow.setProcedure(procedure);
            return flow.<T>select(parameters, aResultSetProcessor, onSuccess != null ? (T t) -> {
                aCallbacksExecutor.execute(() -> {
                    onSuccess.accept(t);
                });
            } : null, onFailure != null ? (Exception ex) -> {
                aCallbacksExecutor.execute(() -> {
                    onFailure.accept(ex);
                });
            } : null);
        } else {
            return null;
        }
    }

    public Command prepareCommand() {
        Command command = new Command(entityName, sqlClause);
        for (int i = 0; i < parameters.size(); i++) {
            Parameter param = parameters.get(i);
            command.getParameters().add(new NamedValue(param.getName(), param.getValue()));
        }
        return command;
    }

    public Command prepareCommand(Map<String, NamedValue> aParameters) {
        Command command = new Command(entityName, sqlClause);
        for (int i = 0; i < parameters.size(); i++) {
            Parameter param = parameters.get(i);
            command.getParameters().add(aParameters.get(param.getName()));
        }
        return command;
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
                            } catch (SQLException ex) {
                                connection.rollback();
                                throw ex;
                            }
                        }
                    } finally {
                        connection.setAutoCommit(autoCommit);
                    }
                }
            } catch (Exception ex) {
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
