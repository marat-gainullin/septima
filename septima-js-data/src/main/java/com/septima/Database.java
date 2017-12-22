package com.septima;

import com.septima.changes.Change;
import com.septima.dataflow.StatementsGenerator;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Field;
import com.septima.metadata.JdbcColumn;
import com.septima.entities.SqlEntity;
import com.septima.sqldrivers.SqlDriver;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database {

    private static final Map<String, Database> DATABASES = new ConcurrentHashMap<>();

    private final DataSource dataSource;
    private final Metadata metadata;
    private final SqlDriver sqlDriver;
    private final Entities entities;
    private final Executor jdbcPerformer;
    private final Executor futureExecutor;

    private Database(DataSource aDataSource, Metadata aMetadata, SqlDriver aSqlDriver, Entities aEntities, Executor aJdbcPerformer, Executor aFuturesExecutor) {
        Objects.requireNonNull(aDataSource, "aDataSource is required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer is required argument");
        Objects.requireNonNull(aFuturesExecutor, "aFuturesExecutor is required argument");
        dataSource = aDataSource;
        metadata = aMetadata;
        sqlDriver = aSqlDriver;
        jdbcPerformer = aJdbcPerformer;
        futureExecutor = aFuturesExecutor;
        entities = aEntities != null ? aEntities : new TablesEntities();
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

    public Executor getFutureExecutor() {
        return futureExecutor;
    }

    public DynamicDataProvider createDataProvider(String aEntityName, String aSqlClause, boolean aProcedure, int aPageSize, Map<String, Field> aExpectedFields) {
        return new DynamicDataProvider(
                metadata.getSqlDriver(),
                aEntityName,
                dataSource,
                jdbcPerformer,
                futureExecutor,
                aSqlClause,
                aProcedure,
                aPageSize,
                aExpectedFields
        );
    }

    public CompletableFuture<Integer> commit(List<Change.Applicable> aChangeLog) {
        Objects.requireNonNull(aChangeLog);
        CompletableFuture<Integer> committing = new CompletableFuture<>();
        jdbcPerformer.execute(() -> {
            try (Connection connection = dataSource.getConnection()) {
                boolean autoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    List<StatementsGenerator.GeneratedStatement> statements = new ArrayList<>();
                    for (Change.Applicable change : aChangeLog) {
                        StatementsGenerator generator = new StatementsGenerator(entities, metadata, sqlDriver);
                        change.accept(generator);
                        statements.addAll(generator.getLogEntries());
                    }
                    int affected = riddleStatements(statements, connection);
                    committing.completeAsync(() -> affected, futureExecutor);
                } catch (SQLException | UncheckedSQLException ex) {
                    connection.rollback();
                    throw ex;
                } finally {
                    connection.setAutoCommit(autoCommit);
                }
            } catch (SQLException | UncheckedSQLException ex) {
                futureExecutor.execute(() -> committing.completeExceptionally(ex));
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

    private static DataSource obtainDataSource(String aDataSourceName) throws NamingException {
        Objects.requireNonNull(aDataSourceName, "aDataSourceName is required argument");
        Context initContext = new InitialContext();
        try {
            return (DataSource) initContext.lookup(aDataSourceName);
        } finally {
            initContext.close();
        }
    }

    public static Database of(final String aDataSourceName) {
        return of(aDataSourceName, 32);
    }

    public static Database of(final String aDataSourceName, int aMaxParallelQueries) {
        return of(
                aDataSourceName,
                jdbcTasksPerformer(Math.max(1, aMaxParallelQueries))
        );
    }

    public static Database of(final String aDataSourceName, Executor aJdbcPerformer) {
        return of(
                aDataSourceName,
                aJdbcPerformer,
                ForkJoinPool.commonPool()
        );
    }

    public static Database of(final String aDataSourceName, Executor aJdbcPerformer, Executor aFutureExecutor) {
        return of(
                aDataSourceName,
                aJdbcPerformer,
                aFutureExecutor,
                null
        );
    }

    public static Database of(final String aDataSourceName, Executor aJdbcPerformer, Executor aFutureExecutor, Entities aEntities) {
        Objects.requireNonNull(aDataSourceName, "aDataSourceName ia required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer ia required argument");
        return DATABASES.computeIfAbsent(aDataSourceName, dsn -> {
            try {
                DataSource ds = obtainDataSource(aDataSourceName);
                return new Database(
                        ds,
                        Metadata.of(ds),
                        DataSources.getDataSourceSqlDriver(ds),
                        aEntities,
                        aJdbcPerformer,
                        aFutureExecutor
                );
            } catch (NamingException ex) {
                throw new IllegalStateException(ex);
            } catch (SQLException ex) {
                throw new UncheckedSQLException(ex);
            }
        });
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

    private class TablesEntities implements Entities {

        @Override
        public SqlEntity loadEntity(String aEntityName) {
            return null;
        }

        @Override
        public SqlEntity loadEntity(String aEntityName, Set<String> illegalReferences) {
            return null;
        }

        @Override
        public Parameter resolveParameter(String aEntityName, String aParamName) {
            return null;
        }

        @Override
        public Field resolveField(String anEntityName, String aFieldName) throws SQLException {
            if (anEntityName != null) {
                Map<String, JdbcColumn> fields = metadata.getTableColumns(anEntityName)
                        .orElseGet(() -> {
                            Logger.getLogger(Database.class.getName()).log(Level.WARNING, "Can't find fields for entity '{0}'", anEntityName);
                            return Map.of();
                        });
                return fields.get(aFieldName);
            } else {
                return null;
            }
        }

        @Override
        public Path getApplicationPath() {
            return null;
        }

        @Override
        public String getDefaultDataSource() {
            return null;
        }
    }
}
