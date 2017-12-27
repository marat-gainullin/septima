package com.septima.dataflow;

import com.septima.metadata.Parameter;
import com.septima.jdbc.JdbcDataProvider;
import com.septima.jdbc.JdbcReaderAssigner;
import com.septima.jdbc.ResultSetReader;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class DynamicTypingDataProvider extends JdbcDataProvider {

    private static final String BAD_PULL_NEXT_PAGE_CHAIN_MSG = "The call indices nextPage() method is allowed only for paged data providers as the subsequent calls in the pull() -> nextPage() -> nextPage() -> ... calls chain";

    private final String entityName;

    public DynamicTypingDataProvider(JdbcReaderAssigner aJdbcReaderAssigner, String aEntityName, DataSource aDataSource, Executor aDataPuller, Executor aFutureExecutor, String aClause, boolean aProcedure, int aPageSize, Map<String, EntityField> aExpectedFields) {
        super(aDataSource, aJdbcReaderAssigner, aDataPuller, aFutureExecutor, aClause, aProcedure, aPageSize, aExpectedFields);
        entityName = aEntityName;
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
                        jdbcReaderAssigner
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
            asyncDataPuller.execute(() -> {
                try {
                    ResultSetReader reader = new ResultSetReader(
                            expectedFields,
                            jdbcReaderAssigner
                    );
                    Collection<Map<String, Object>> processed = reader.readRowSet(lowLevelResults, pageSize);
                    fetching.completeAsync(() -> processed, futureExecutor);
                } catch (Throwable ex) {
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

}
