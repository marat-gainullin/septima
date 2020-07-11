package com.septima;

import com.septima.dataflow.DynamicTypingDataProvider;
import com.septima.dataflow.EntityActionsBinder;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.JdbcReaderAssigner;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import com.septima.sqldrivers.SqlDriver;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Database {

    private final DataSource dataSource;
    private final Metadata metadata;
    private final SqlDriver sqlDriver;
    private final JdbcReaderAssigner procedureAssigner;
    private final JdbcReaderAssigner nonProcedureAssigner;
    private final boolean useBatches;
    private final int maximumBatchSize;
    private final Executor jdbcPerformer;
    private final Executor futuresExecutor;

    public Database(DataSource aDataSource, SqlDriver aSqlDriver, Metadata aMetadata, Executor aJdbcPerformer, Executor aFuturesExecutor, boolean aUseBatches, int aMaximumBatchSize) {
        Objects.requireNonNull(aDataSource, "aDataSource is required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer is required argument");
        Objects.requireNonNull(aFuturesExecutor, "aFuturesExecutor is required argument");
        dataSource = aDataSource;
        metadata = aMetadata;
        sqlDriver = aSqlDriver;
        jdbcPerformer = aJdbcPerformer;
        futuresExecutor = aFuturesExecutor;
        procedureAssigner = new JdbcReaderAssigner(sqlDriver, true);
        nonProcedureAssigner = new JdbcReaderAssigner(sqlDriver, false);
        useBatches = aUseBatches;
        maximumBatchSize = aMaximumBatchSize;
    }

    private int applyStatements(List<EntityActionsBinder.BoundStatement> aStatements, Connection aConnection) throws SQLException {
        if (useBatches) {
            return applyStatementsAsBatches(aStatements, aConnection);
        } else {
            return applyStatementsOneByOne(aStatements, aConnection);
        }
    }

    private int applyStatementsOneByOne(List<EntityActionsBinder.BoundStatement> aStatements, Connection aConnection) throws SQLException {
        int rowsAffected = 0;
        for (EntityActionsBinder.BoundStatement entry : aStatements) {
            rowsAffected += entry.apply(aConnection);
        }
        return rowsAffected;
    }

    private static class StatementsBatch {
        private final Connection connection;
        private final PreparedStatement stmt;
        private final String clause;

        private StatementsBatch(Connection aConnection, PreparedStatement aStmt, String aClause) {
            connection = aConnection;
            stmt = aStmt;
            clause = aClause;
        }

        public boolean clauseNotSame(String aClause) {
            return !Objects.equals(clause, aClause);
        }

        public void add(EntityActionsBinder.BoundStatement entry) throws SQLException {
            entry.assignParameters(connection, stmt);
            stmt.addBatch();
        }

        public int flush() throws SQLException {
            return Arrays.stream(stmt.executeBatch()).sum();
        }

        public void close() throws SQLException {
            stmt.close();
        }

        public static StatementsBatch of(Connection aConnection, String aClause) throws SQLException {
            return new StatementsBatch(aConnection, aConnection.prepareStatement(aClause), aClause);
        }
    }

    private int applyStatementsAsBatches(List<EntityActionsBinder.BoundStatement> aStatements, Connection aConnection) throws SQLException {
        int rowsAffected = 0;
        if (!aStatements.isEmpty()) {
            EntityActionsBinder.BoundStatement firstStatement = aStatements.get(0);
            StatementsBatch batch = StatementsBatch.of(aConnection, firstStatement.getClause());
            try {
                batch.add(firstStatement);
                for (int i = 1; i < aStatements.size(); i++) {
                    EntityActionsBinder.BoundStatement statement = aStatements.get(i);
                    if (batch.clauseNotSame(statement.getClause()) || i % maximumBatchSize == 0) {
                        rowsAffected += batch.flush();
                        try {
                            batch.close();
                        } finally { // Try .. finally here to avoid exception hiding by second attempt to call statement.close()
                            batch = null;
                        }
                        batch = StatementsBatch.of(aConnection, statement.getClause());
                    }
                    batch.add(statement);
                }
                rowsAffected += batch.flush();
            } finally {
                if (batch != null) { // Check here to avoid exception hiding by second attempt to call statement.close()
                    batch.close();
                }
            }
        }
        return rowsAffected;
    }

    public static DataSource obtainDataSource(String aDataSourceName) throws NamingException {
        Objects.requireNonNull(aDataSourceName, "aDataSourceName is required argument");
        try {
            return (DataSource) InitialContext.doLookup("java:comp/env/" + aDataSourceName);
        } catch (NamingException ex) {
            try {
                return (DataSource) InitialContext.doLookup("java:comp/" + aDataSourceName);
            } catch (NamingException ex1) {
                return (DataSource) InitialContext.doLookup(aDataSourceName);
            }
        }
    }

    public static ExecutorService jdbcTasksPerformer(final int aMaxParallelQueries) {
        AtomicLong threadNumber = new AtomicLong();
        ThreadPoolExecutor jdbcProcessor = new ThreadPoolExecutor(aMaxParallelQueries, aMaxParallelQueries,
                3L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r -> new Thread(r, "jdbc-" + threadNumber.incrementAndGet()));
        jdbcProcessor.allowCoreThreadTimeOut(true);
        return jdbcProcessor;
//        jdbc.shutdown();
//        jdbc.awaitTermination(30L, TimeUnit.SECONDS);
    }

    public static Database of(String aDataSourceName, int aMaximumJdbcThreads, boolean useBatches, int aMaximumBatchSize) throws NamingException, SQLException {
        DataSource dataSource = obtainDataSource(aDataSourceName);
        Metadata metadata = Metadata.of(dataSource);
        return new Database(
                dataSource,
                DataSources.getDataSourceSqlDriver(dataSource),
                metadata,
                jdbcTasksPerformer(aMaximumJdbcThreads),
                ForkJoinPool.commonPool(),
                useBatches,
                aMaximumBatchSize
        );
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public SqlDriver getSqlDriver() {
        return sqlDriver;
    }

    public Executor getJdbcPerformer() {
        return jdbcPerformer;
    }

    public Executor getFuturesExecutor() {
        return futuresExecutor;
    }

    public DynamicTypingDataProvider createDataProvider(String aEntityName, String aSqlClause, boolean aProcedure, int aPageSize, Map<String, EntityField> aExpectedFields) {
        return new DynamicTypingDataProvider(
                jdbcReaderAssigner(aProcedure),
                aEntityName,
                dataSource,
                jdbcPerformer,
                futuresExecutor,
                aSqlClause,
                aProcedure,
                aPageSize,
                aExpectedFields
        );
    }

    public JdbcReaderAssigner jdbcReaderAssigner(boolean aProcedure) {
        return aProcedure ? procedureAssigner : nonProcedureAssigner;
    }

    public CompletableFuture<Integer> commit(List<EntityActionsBinder.BoundStatement> statements) {
        Objects.requireNonNull(statements);
        CompletableFuture<Integer> committing = new CompletableFuture<>();
        jdbcPerformer.execute(() -> {
            try (Connection connection = dataSource.getConnection()) {
                boolean autoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    int affected = applyStatements(statements, connection);
                    connection.commit();
                    committing.completeAsync(() -> affected, futuresExecutor);
                } catch (SQLException | UncheckedSQLException ex) {
                    connection.rollback();
                    throw ex;
                } finally {
                    connection.setAutoCommit(autoCommit);
                }
            } catch (Throwable ex) {
                futuresExecutor.execute(() -> committing.completeExceptionally(ex));
            }
        });
        return committing;
    }
}
