package com.septima.jdbc;

import com.septima.sqldrivers.SqlDriver;

import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * @author mg
 */
public class DataSources {

    private DataSources() {
        super();
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

    public static String getDataSourceSchema(DataSource aDataSource) throws SQLException {
        try (Connection conn = aDataSource.getConnection()) {
            return schemaByConnection(conn);
        }
    }

    public static SqlDriver getDataSourceSqlDriver(DataSource aDataSource) throws SQLException {
        try (Connection conn = aDataSource.getConnection()) {
            return sqlDriverByConnection(conn);
        }
    }

    private static SqlDriver sqlDriverByConnection(Connection aConnection) throws SQLException {
        return SqlDriver.of(aConnection.getMetaData().getURL());
    }

    private static String schemaByConnection(Connection aConnection) throws SQLException {
        SqlDriver driver = sqlDriverByConnection(aConnection);
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
}
