package com.septima.application;

import com.septima.Config;
import com.septima.Database;
import com.septima.entities.SqlEntities;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class Data {

    private final SqlEntities entities;

    private Data(SqlEntities aEntities) {
        entities = aEntities;
    }

    public SqlEntities getEntities() {
        return entities;
    }

    private static volatile Data instance;

    private static void init(Config aConfig) {
        instance = new Data(new SqlEntities(aConfig.getEntitiesPath(), aConfig.getDefaultDataSourceName(), Database.jdbcTasksPerformer(aConfig.getMaximumJdbcThreads()), Config.lookupExecutor()));
    }

    private static void done() {
        instance = null;
    }

    public static Data getInstance() {
        return instance;
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
