package com.septima;

import com.septima.changes.Change;
import com.septima.dataflow.StatementsGenerator;
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

    public SeptimaDataProvider createDataProvider(String aEntityName, String aSqlClause, Map<String, Field> aExpectedFields) throws Exception {
        return new SeptimaDataProvider(metadata.getDataSourceSqlDriver(), aEntityName, dataSource, jdbcPerformer, aSqlClause, aExpectedFields);
    }

    public CompletableFuture<Integer> commit(List<Change.Applicable> aChangeLog) {
        assert aChangeLog != null;
        CompletableFuture<Integer> commiting = new CompletableFuture<>();
        jdbcPerformer.accept(() -> {
            try {
                try(Connection connection = dataSource.getConnection()) {
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
                        commiting.completeAsync(() -> affected, futureExecutor);
                    } catch (Exception ex) {
                        connection.rollback();
                        throw ex;
                    } finally {
                        connection.setAutoCommit(autoCommit);
                    }
                }
            } catch (Exception ex) {
                futureExecutor.execute(() -> commiting.completeExceptionally(ex));
            }
        });
        return commiting;
    }

    private static int riddleStatements(List<StatementsGenerator.GeneratedStatement> aStatements, Connection aConnection) throws Exception {
        int rowsAffected = 0;
        if (!aStatements.isEmpty()) {
            List<StatementsGenerator.GeneratedStatement> errorStatements = new ArrayList<>();
            List<String> errors = new ArrayList<>();
            for (StatementsGenerator.GeneratedStatement entry : aStatements) {
                try {
                    rowsAffected += entry.apply(aConnection);
                } catch (Exception ex) {
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

    private static DataSource obtainDataSource(String aDataSourceName) throws Exception {
        if (aDataSourceName != null && !aDataSourceName.isEmpty()) {
            Context initContext = new InitialContext();
            try {
                // J2EE servers
                return (DataSource) initContext.lookup(aDataSourceName);
            } catch (NamingException ex) {
                // Apache Tomcat component's JNDI context
                Context envContext = (Context) initContext.lookup("java:/comp/env"); //NOI18N
                return (DataSource) envContext.lookup(aDataSourceName);
            }
        } else {
            throw new NamingException("Data source name missing.");
        }
    }

    public static Database of(final String aDataSourceName, EntitiesHost aEntitiesHost, Consumer<Runnable> aJdbcPerformer, Executor aFutureExecutor) throws Exception {
        assert aDataSourceName != null : "aDataSourceName ia required argument";
        assert aJdbcPerformer != null : "aJdbcPerformer ia required argument";
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
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
    }
}
