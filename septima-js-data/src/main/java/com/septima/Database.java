package com.septima;

import com.septima.application.ApplicationDataProvider;
import com.septima.changes.Change;
import com.septima.dataflow.StatementsGenerator;
import com.septima.jdbc.DataSources;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.Field;
import com.septima.metadata.JdbcColumn;
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

    private static final Map<String, Database> DATABASES = new ConcurrentHashMap<>();

    private final DataSource dataSource;
    private final Metadata metadata;
    private final SqlDriver sqlDriver;
    private final EntitiesHost entitiesHost;
    private final Executor jdbcPerformer;
    private final Executor futureExecutor;

    private Database(DataSource aDataSource, Metadata aMetadata, SqlDriver aSqlDriver, EntitiesHost aEntitiesHost, Executor aJdbcPerformer, Executor aFuturesExecutor) {
        Objects.requireNonNull(aDataSource, "aDataSource is required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer is required argument");
        Objects.requireNonNull(aFuturesExecutor, "aFuturesExecutor is required argument");
        dataSource = aDataSource;
        metadata = aMetadata;
        sqlDriver = aSqlDriver;
        jdbcPerformer = aJdbcPerformer;
        futureExecutor = aFuturesExecutor;
        entitiesHost = aEntitiesHost != null ? aEntitiesHost : new TablesEntities();
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

    public ApplicationDataProvider createDataProvider(String aEntityName, String aSqlClause, boolean aProcedure, int aPageSize, Map<String, Field> aExpectedFields) {
        return new ApplicationDataProvider(
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
                        StatementsGenerator generator = new StatementsGenerator(entitiesHost, metadata, sqlDriver);
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
        Objects.requireNonNull(aDataSourceName, "Data source name missing.");
        Context initContext = new InitialContext();
        try {
            // J2EE servers
            return (DataSource) initContext.lookup(aDataSourceName);
        } catch (NamingException ex) {
            // Apache Tomcat component's JNDI context
            Context envContext = (Context) initContext.lookup("java:/comp/env"); //NOI18N
            return (DataSource) envContext.lookup(aDataSourceName);
        }
    }

    public static Database of(final String aDataSourceName) {
        return of(
                aDataSourceName,
                32
        );
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

    public static Database of(final String aDataSourceName, Executor aJdbcPerformer, Executor aFutureExecutor, EntitiesHost aEntitiesHost) {
        Objects.requireNonNull(aDataSourceName, "aDataSourceName ia required argument");
        Objects.requireNonNull(aJdbcPerformer, "aJdbcPerformer ia required argument");
        return DATABASES.computeIfAbsent(aDataSourceName, dsn -> {
            try {
                DataSource ds = obtainDataSource(aDataSourceName);
                return new Database(
                        ds,
                        Metadata.of(ds),
                        DataSources.getDataSourceSqlDriver(ds),
                        aEntitiesHost,
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

    private class TablesEntities implements EntitiesHost {

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

    }
}
