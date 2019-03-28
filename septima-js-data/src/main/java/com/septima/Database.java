package com.septima;

import com.septima.dataflow.DynamicTypingDataProvider;
import com.septima.dataflow.EntityActionsBinder;
import com.septima.jdbc.JdbcReaderAssigner;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import com.septima.sqldrivers.SqlDriver;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Database {

    private final DataSource dataSource;
    private final Metadata metadata;
    private final SqlDriver sqlDriver;
    private final Executor jdbcPerformer;
    private final Executor futuresExecutor;

    public Database(DataSource aDataSource, Metadata aMetadata, Executor aJdbcPerformer, Executor aFuturesExecutor) {
        Objects.requireNonNull(aDataSource, "aDataSource is required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer is required argument");
        Objects.requireNonNull(aFuturesExecutor, "aFuturesExecutor is required argument");
        dataSource = aDataSource;
        metadata = aMetadata;
        sqlDriver = metadata.getSqlDriver();
        jdbcPerformer = aJdbcPerformer;
        futuresExecutor = aFuturesExecutor;
    }

    private static int applyStatements(List<EntityActionsBinder.BoundStatement> aStatements, Connection aConnection) throws SQLException {
        int rowsAffected = 0;
        for (EntityActionsBinder.BoundStatement entry : aStatements) {
            rowsAffected += entry.apply(aConnection);
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

    public static Database of(String aDataSourceName) throws NamingException, SQLException {
        DataSource dataSource = obtainDataSource(aDataSourceName);
        Metadata metadata = Metadata.of(dataSource);
        return new Database(dataSource, metadata, jdbcTasksPerformer(32), ForkJoinPool.commonPool());
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
        return new JdbcReaderAssigner(sqlDriver, aProcedure);
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
