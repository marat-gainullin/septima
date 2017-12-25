package com.septima.dataflow;

import com.septima.DataTypes;
import com.septima.Parameter;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Field;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This flow dataSource intended transform support the data flow process from jdbc
 * data sources.
 *
 * @author mg
 * @see DataProvider
 */
public abstract class JdbcDataProvider implements DataProvider {

    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final Logger queriesLogger = Logger.getLogger(JdbcDataProvider.class.getName());

    public interface ResultSetProcessor<T> {
        T apply(ResultSet aData) throws SQLException;
    }

    private final String clause;
    private final DataSource dataSource;
    private final boolean procedure;
    protected final StatementResultSetHandler statementResultSetHandler;

    protected final Executor asyncDataPuller;
    protected final Executor futureExecutor;
    protected final int pageSize;

    protected final Map<String, Field> expectedFields;

    private Connection lowLevelConnection;
    private PreparedStatement lowLevelStatement;

    protected ResultSet lowLevelResults;

    /**
     * A flow dataSource, intended transform support jdbc data sources.
     *
     * @param aDataSource      A DataSource instance, that would supply resources for
     *                         use them by flow dataSource in single operations, like retriving data of
     *                         applying data changes.
     * @param aStatementResultSetHandler Jdbc {@link PreparedStatement} and {@link CallableStatement} parameters handler.
     * @param aAsyncDataPuller {@link Executor} for Jdbc blocking tasks.
     * @param aClause          A sql clause, dataSource should use transform achieve
     *                         PreparedStatement instance transform use it in the result set querying process.
     * @param aExpectedFields  Fields, expected by Septima according transform metadata analysis.
     * @see DataSource
     */
    public JdbcDataProvider(DataSource aDataSource, StatementResultSetHandler aStatementResultSetHandler, Executor aAsyncDataPuller, Executor aFutureExecutor, String aClause, boolean aProcedure, int aPageSize, Map<String, Field> aExpectedFields) {
        super();
        Objects.requireNonNull(aClause, "Flow provider cant't exist without a selecting sql clause");
        Objects.requireNonNull(aDataSource, "Flow provider can't exist without a data source");
        Objects.requireNonNull(aStatementResultSetHandler, "aStatementResultSetHandler is required argument");
        dataSource = aDataSource;
        statementResultSetHandler = aStatementResultSetHandler;
        asyncDataPuller = aAsyncDataPuller;
        futureExecutor = aFutureExecutor;
        clause = aClause;
        procedure = aProcedure;
        pageSize = aPageSize;
        expectedFields = aExpectedFields;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    protected boolean isPaged() {
        return pageSize > 0;
    }

    @Override
    public boolean isProcedure() {
        return procedure;
    }

    protected <T> CompletableFuture<T> select(List<Parameter> aParams, ResultSetProcessor<T> aProcessor) {
        CompletableFuture<T> fetching = new CompletableFuture<>();
        if (isPaged()) {
            endPaging();
        }
        asyncDataPuller.execute(() -> {
            try {
                String sqlClause = clause;
                Connection connection = dataSource.getConnection();
                try {
                    PreparedStatement statement = getFlowStatement(connection, sqlClause);
                    try {
                        Map<Integer, Integer> assignedJdbcTypes = new HashMap<>();
                        for (int i = 1; i <= aParams.size(); i++) {
                            Parameter param = aParams.get(i - 1);
                            int assignedJdbcType = statementResultSetHandler.assignInParameter(param, statement, i, connection);
                            assignedJdbcTypes.put(i, assignedJdbcType);
                        }
                        logQuery(sqlClause, aParams, assignedJdbcTypes);
                        ResultSet results;
                        if (procedure) {
                            assert statement instanceof CallableStatement;
                            CallableStatement cStmt = (CallableStatement) statement;
                            cStmt.execute();
                            // let's return parameters
                            for (int i = 1; i <= aParams.size(); i++) {
                                Parameter param = aParams.get(i - 1);
                                statementResultSetHandler.acceptOutParameter(param, cStmt, i, connection);
                            }
                            // let's return a ResultSet
                            results = cStmt.getResultSet();
                        } else {
                            results = statement.executeQuery();
                        }
                        try {
                            T processed = aProcessor.apply(results/* may be null*/);
                            fetching.completeAsync(() -> processed, futureExecutor);
                        } finally {
                            if (isPaged()) {
                                lowLevelResults = results;/* may be null*/
                            } else {
                                /* may be null because of CallableStatement*/
                                if (results != null) {
                                    results.close();
                                }
                            }
                        }
                    } finally {
                        if (lowLevelResults != null) {
                            // Paged statements can't be closed, because of ResultSet existence.
                            lowLevelStatement = statement;
                        } else {
                            statement.close();
                        }
                    }
                } finally {
                    if (lowLevelStatement != null) {
                        // Paged connections can't be closed, because of ResultSet existence.
                        lowLevelConnection = connection;
                    } else {
                        connection.close();
                    }
                }
            } catch (Throwable ex) {
                futureExecutor.execute(() -> fetching.completeExceptionally(ex));
            }
        });
        return fetching;
    }

    protected void endPaging() {
        assert isPaged();
        close();
    }

    @Override
    public void close() {
        try {
            if (lowLevelResults != null) {
                lowLevelResults.close();
                lowLevelResults = null;
            }
            // See pull method, hacky statement closing.
            if (lowLevelStatement != null) {
                lowLevelStatement.close();
                lowLevelStatement = null;
            }
            // See pull method, hacky connection closing.
            if (lowLevelConnection != null) {
                lowLevelConnection.close();
                lowLevelConnection = null;
            }
        } catch (SQLException ex) {
            throw new UncheckedSQLException(ex);
        }
    }

    private static void logQuery(String sqlClause, List<Parameter> aParams, Map<Integer, Integer> aAssignedJdbcTypes) {
        if (queriesLogger.isLoggable(Level.FINE)) {
            boolean finerLogs = queriesLogger.isLoggable(Level.FINER);
            queriesLogger.log(Level.FINE, "Executing sql:\n{0}\nwith {1} parameters{2}", new Object[]{sqlClause, aParams.size(), finerLogs && !aParams.isEmpty() ? ":" : ""});
            if (finerLogs) {
                for (int i = 1; i <= aParams.size(); i++) {
                    Parameter param = aParams.get(i - 1);
                    Object paramValue = param.getValue();
                    if (paramValue != null && DataTypes.DATE_TYPE_NAME.equals(param.getType())) {
                        java.util.Date dateValue = (java.util.Date) paramValue;
                        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
                        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                        String jsonLikeText = sdf.format(dateValue);
                        queriesLogger.log(Level.FINER, "order: {0}; name: {1}; jdbc type: {2}; json like timestamp: {3}; raw timestamp: {4};", new Object[]{i, param.getName(), aAssignedJdbcTypes.get(i), jsonLikeText, dateValue.getTime()});
                    } else {// nulls, String, Number, Boolean
                        queriesLogger.log(Level.FINER, "order: {0}; name: {1}; jdbc type: {2}; value: {3};", new Object[]{i, param.getName(), aAssignedJdbcTypes.get(i), param.getValue()});
                    }
                }
            }
        }
    }

    /**
     * Returns PreparedStatement instance. Let's consider some caching system.
     * It will provide some prepared statement instance, according transform passed sql
     * clause.
     *
     * @param aConnection java.sql.Connection instance transform be used.
     * @param aClause     Sql clause transform process.
     * @return StatementResourceDescriptor instance, provided according transform sql
     * clause.
     */
    private PreparedStatement getFlowStatement(Connection aConnection, String aClause) throws SQLException {
        assert aConnection != null;
        if (procedure) {
            return aConnection.prepareCall(aClause);
        } else {
            return aConnection.prepareStatement(aClause);
        }
    }

}
