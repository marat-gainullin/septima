package com.septima.application;

import com.septima.Database;
import com.septima.entities.SqlEntities;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class Data {

    private static volatile Data instance;
    private final SqlEntities entities;

    private Data(SqlEntities aEntities) {
        entities = aEntities;
    }

    private static void init(Config aConfig) {
        if (instance != null) {
            throw new IllegalStateException("Data can be initialized only once.");
        }
        instance = new Data(new SqlEntities(
                aConfig.getResourcesEntitiesPath(),
                aConfig.getEntitiesPath(),
                aConfig.getDefaultDataSourceName(),
                Database.jdbcTasksPerformer(aConfig.getMaximumJdbcThreads()),
                Config.lookupExecutor(),
                Boolean.valueOf(System.getProperty("com.septima.entities.compile", "false"))
        ));
    }

    private static void done() {
        if (instance == null) {
            throw new IllegalStateException("Extra data shutdown attempt detected.");
        }
        instance = null;
    }

    public static Data getInstance() {
        return instance;
    }

    public SqlEntities getEntities() {
        return entities;
    }

    public static class Init implements ServletContextListener {
        @Override
        public void contextInitialized(ServletContextEvent anEvent) {
            init(Config.parse(anEvent.getServletContext()));
        }

        @Override
        public void contextDestroyed(ServletContextEvent anEvent) {
            done();
        }
    }
}
