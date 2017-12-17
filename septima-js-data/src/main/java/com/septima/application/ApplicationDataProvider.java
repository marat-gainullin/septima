package com.septima.application;

import com.septima.Parameter;
import com.septima.dataflow.NotPagedException;
import com.septima.dataflow.JdbcDataProvider;
import com.septima.dataflow.ResultSetReader;
import com.septima.jdbc.NamedJdbcValue;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Field;
import com.septima.sqldrivers.SqlDriver;

import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.sql.DataSource;

public class ApplicationDataProvider extends JdbcDataProvider {

    private static final String BAD_PULL_NEXT_PAGE_CHAIN_MSG = "The call of nextPage() method is allowed only for paged data providers as the subsequent calls in the pull() -> nextPage() -> nextPage() -> ... calls chain";

    private final String entityName;
    private final SqlDriver sqlDriver;

    public ApplicationDataProvider(SqlDriver aSqlDriver, String aEntityName, DataSource aDataSource, Consumer<Runnable> aDataPuller, Executor aFutureExecutor, String aClause, boolean aProcedure, int aPageSize, Map<String, Field> aExpectedFields) {
        super(aDataSource, aDataPuller, aFutureExecutor, aClause, aProcedure, aPageSize, aExpectedFields);
        entityName = aEntityName;
        sqlDriver = aSqlDriver;
    }

    @Override
    public String getEntityName() {
        return entityName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Collection<Map<String, Object>>> pull(List<Parameter> aParams) {
        return select(aParams, (ResultSet rs) -> {
            if (rs != null) {
                ResultSetReader reader = new ResultSetReader(
                        expectedFields,
                        sqlDriver::readGeometry,
                        sqlDriver.getTypesResolver()::toApplicationType
                );
                return reader.readRowSet(rs, pageSize);
            } else {
                return List.of();
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Collection<Map<String, Object>>> nextPage() throws NotPagedException {
        if (!isPaged() || lowLevelResults == null) {
            throw new NotPagedException(BAD_PULL_NEXT_PAGE_CHAIN_MSG);
        } else {
            CompletableFuture<Collection<Map<String, Object>>> fetching = new CompletableFuture<>();
            asyncDataPuller.accept(() -> {
                try {
                    ResultSetReader reader = new ResultSetReader(
                            expectedFields,
                            sqlDriver::readGeometry,
                            sqlDriver.getTypesResolver()::toApplicationType
                    );
                    Collection<Map<String, Object>> processed = reader.readRowSet(lowLevelResults, pageSize);
                    fetching.completeAsync(() -> processed, futureExecutor);
                } catch (SQLException | UncheckedSQLException ex) {
                    futureExecutor.execute(() -> fetching.completeExceptionally(ex));
                } finally {
                    try {
                        if (lowLevelResults.isClosed() || lowLevelResults.isAfterLast()) {
                            endPaging();
                        }
                    } catch (SQLException ex) {
                        throw new UncheckedSQLException(ex);
                    }
                }
            });
            return fetching;
        }
    }

    @Override
    protected int assignParameter(Parameter aParameter, PreparedStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        if (ApplicationDataTypes.GEOMETRY_TYPE_NAME.equals(aParameter.getType())) {
            NamedJdbcValue jv = sqlDriver.convertGeometry(aParameter.getValue().toString(), aConnection);
            Object paramValue = jv.getValue();
            int jdbcType = jv.getJdbcType();
            String sqlTypeName = jv.getSqlTypeName();
            int assignedJdbcType = assign(paramValue, aParameterIndex, aStatement, jdbcType, sqlTypeName);
            checkOutParameter(aParameter, aStatement, aParameterIndex, jdbcType);
            return assignedJdbcType;
        } else {
            return super.assignParameter(aParameter, aStatement, aParameterIndex, aConnection);
        }
    }

    @Override
    protected void acceptOutParameter(Parameter aParameter, CallableStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        if (ApplicationDataTypes.GEOMETRY_TYPE_NAME.equals(aParameter.getType())) {
            String sGeometry = sqlDriver.readGeometry(aStatement, aParameterIndex, aConnection);
            aParameter.setValue(sGeometry);
        } else {
            super.acceptOutParameter(aParameter, aStatement, aParameterIndex, aConnection);
        }
    }
}
