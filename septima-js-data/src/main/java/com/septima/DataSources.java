package com.septima;

import com.septima.sqldrivers.GenericSqlDriver;
import com.septima.sqldrivers.SqlDriver;

import java.sql.*;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * @author mg
 */
public class DataSources {

    private static final SqlDriver GENERIC_DRIVER = new GenericSqlDriver();
    private static final Set<SqlDriver> DRIVERS = new ConcurrentHashMap<SqlDriver, Boolean>() {
        {
            ServiceLoader<SqlDriver> loader = ServiceLoader.load(SqlDriver.class);
            Iterator<SqlDriver> drivers = loader.iterator();
            drivers.forEachRemaining(sqlDriver -> {
                try {
                    put(sqlDriver, true);
                } catch (Throwable t) {
                    Logger.getLogger(DataSources.class.getName()).log(Level.WARNING, null, t);
                }
            });
        }
    }.keySet();

    private DataSources() {
        super();
    }

    private static SqlDriver getSqlDriver(String aDialect) {
        return DRIVERS.stream()
                .filter(sqlDriver -> sqlDriver.is(aDialect))
                .findFirst()
                .orElse(GENERIC_DRIVER);
    }

    public static ExecutorService newJdbcTasksPerformer(final int aMaxParallelQueries) {
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

    public static String getDataSourceSchema(DataSource aDataSource) throws Exception {
        try (Connection conn = aDataSource.getConnection()) {
            return schemaByConnection(conn);
        }
    }

    public static String getDataSourceDialect(DataSource aDataSource) throws Exception {
        try (Connection conn = aDataSource.getConnection()) {
            return dialectByConnection(conn);
        }
    }

    public static SqlDriver getDataSourceSqlDriver(DataSource aDataSource) throws Exception {
        return getSqlDriver(getDataSourceDialect(aDataSource));
    }

    private static String dialectByConnection(Connection aConnection) throws SQLException {
        String dialect = dialectByUrl(aConnection.getMetaData().getURL());
        if (dialect == null) {
            dialect = dialectByProductName(aConnection.getMetaData().getDatabaseProductName());
        }
        return dialect;
    }

    private static String schemaByConnection(Connection aConnection) throws SQLException {
        String dialect = dialectByConnection(aConnection);
        SqlDriver driver = getSqlDriver(dialect);
        if (driver != null) {
            String getSchemaClause = driver.getSql4GetSchema();
            if (getSchemaClause != null) {
                try (Statement stmt = aConnection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(getSchemaClause)) {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                            return rs.getString(1);
                        }
                    }
                }
            } else {
                return null;
            }
        } else {
            Logger.getLogger(DataSources.class.getName()).log(Level.SEVERE, String.format("Can't obtain sql driver for %s", aConnection.toString()));
        }
        return null;
    }

    private static String dialectByUrl(String aJdbcUrl) {
        String dialect = null;
        if (aJdbcUrl != null) {
            aJdbcUrl = aJdbcUrl.toLowerCase();
            if (aJdbcUrl.contains("jdbc:oracle")) { //NOI18N
                dialect = Constants.ORACLE_DIALECT;
            } else if (aJdbcUrl.contains("jdbc:jtds:sqlserver")) { //NOI18N
                dialect = Constants.MSSQL_DIALECT;
            } else if (aJdbcUrl.contains("jdbc:postgre")) { //NOI18N
                dialect = Constants.POSTGRE_DIALECT;
            } else if (aJdbcUrl.contains("jdbc:db2")) { //NOI18N
                dialect = Constants.DB2_DIALECT;
            } else if (aJdbcUrl.contains("jdbc:mysql")) { //NOI18N
                dialect = Constants.MYSQL_DIALECT;
            } else if (aJdbcUrl.contains("jdbc:h2")) { //NOI18N
                dialect = Constants.H2_DIALECT;
            }
        }
        return dialect;
    }

    private static String dialectByProductName(String aName) {
        String dialect = null;
        if (aName != null) {
            aName = aName.toLowerCase();
            if (aName.contains("oracle")) { //NOI18N
                dialect = Constants.ORACLE_DIALECT;
            } else if (aName.contains("microsoft")) { //NOI18N
                dialect = Constants.MSSQL_DIALECT;
            } else if (aName.contains("postgre")) { //NOI18N
                dialect = Constants.POSTGRE_DIALECT;
            } else if (aName.contains("db2")) { //NOI18N
                dialect = Constants.DB2_DIALECT;
            } else if (aName.contains("mysql")) { //NOI18N
                dialect = Constants.MYSQL_DIALECT;
            } else if (aName.contains("h2")) { //NOI18N
                dialect = Constants.H2_DIALECT;
            }
        }
        return dialect;
    }

}
