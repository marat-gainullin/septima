package com.septima;

import com.septima.application.ApplicationDataProvider;
import com.septima.changes.Change;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database {

    private static final Map<String, Database> DATABASES = new ConcurrentHashMap<>();

    private final DataSource dataSource;
    private final Metadata metadata;
    private final SqlDriver sqlDriver;
    private final EntitiesHost entitiesHost;
    private final Consumer<Runnable> jdbcPerformer;
    private final Executor futureExecutor;

    private Database(DataSource aDataSource, Metadata aMetadata, SqlDriver aSqlDriver, EntitiesHost aEntitiesHost, Consumer<Runnable> aJdbcPerfomrer, Executor aFuturesExecutor) {
        dataSource = aDataSource;
        metadata = aMetadata;
        sqlDriver = aSqlDriver;
        jdbcPerformer = aJdbcPerfomrer;
        futureExecutor = aFuturesExecutor;
        entitiesHost = aEntitiesHost;
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

    public Consumer<Runnable> getJdbcPerformer() {
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
        jdbcPerformer.accept(() -> {
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

    public static Database of(final String aDataSourceName, EntitiesHost aEntitiesHost, Consumer<Runnable> aJdbcPerformer, Executor aFutureExecutor) {
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
}
