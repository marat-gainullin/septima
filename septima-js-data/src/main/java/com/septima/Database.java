package com.septima;

import com.septima.dataflow.StatementsGenerator;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Field;
import com.septima.sqldrivers.SqlDriver;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    public DynamicDataProvider createDataProvider(String aEntityName, String aSqlClause, boolean aProcedure, int aPageSize, Map<String, Field> aExpectedFields) {
        return new DynamicDataProvider(
                metadata.getSqlDriver(),
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

    public CompletableFuture<Integer> commit(List<StatementsGenerator.GeneratedStatement> statements) {
        Objects.requireNonNull(statements);
        CompletableFuture<Integer> committing = new CompletableFuture<>();
        jdbcPerformer.execute(() -> {
            try (Connection connection = dataSource.getConnection()) {
                boolean autoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    int affected = riddleStatements(statements, connection);
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

    private static int riddleStatements(List<StatementsGenerator.GeneratedStatement> aStatements, Connection aConnection) throws SQLException {
        int rowsAffected = 0;
        if (!aStatements.isEmpty()) {
            List<StatementsGenerator.GeneratedStatement> errorStatements = new ArrayList<>();
            List<String> errors = new ArrayList<>();
            for (StatementsGenerator.GeneratedStatement entry : aStatements) {
                try {
                    rowsAffected += entry.apply(aConnection);
                } catch (SQLException | UncheckedSQLException ex) {
                    errorStatements.add(entry);
                    errors.add(ex.getMessage());
                    Logger.getLogger(DataSources.class.getName()).log(Level.WARNING, ex.getMessage());
                }
            }
            if (errorStatements.size() == aStatements.size()) {
                throw new SQLException(String.join("\n", errors));
            } else if (errorStatements.size() < aStatements.size()) {
                rowsAffected += riddleStatements(errorStatements, aConnection);
            }
        }
        return rowsAffected;
    }

    public static DataSource obtainDataSource(String aDataSourceName) throws NamingException {
        Objects.requireNonNull(aDataSourceName, "aDataSourceName is required argument");
        Context initContext = new InitialContext();
        try {
            return (DataSource) initContext.lookup(aDataSourceName);
        } finally {
            initContext.close();
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
}
