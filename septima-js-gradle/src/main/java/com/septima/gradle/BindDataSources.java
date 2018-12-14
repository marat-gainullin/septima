package com.septima.gradle;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BindDataSources extends DefaultTask {

    private Map<String, Map<String, String>> dataSourcesSettings = Map.of();

    public Map<String, Map<String, String>> getDataSourcesSettings() {
        return dataSourcesSettings;
    }

    public void setDataSourcesSettings(Map<String, Map<String, String>> dataSourcesSettings) {
        this.dataSourcesSettings = dataSourcesSettings;
    }

    @TaskAction
    public void bindDataSources() {
        List<HikariDataSource> boundDataSources = new ArrayList<>();
        dataSourcesSettings.forEach((dataSourceName, dataSourceSettings) -> {
            try {
                var config = new HikariConfig();
                config.setJdbcUrl(dataSourceSettings.get("url"));
                config.setUsername(dataSourceSettings.get("user"));
                config.setDriverClassName(dataSourceSettings.get("driverClass"));
                if (dataSourceSettings.get("password") != null) {
                    config.setPassword(dataSourceSettings.get("password"));
                }
                if (dataSourceSettings.get("maxConnections") != null) {
                    config.setMaximumPoolSize(Integer.valueOf(dataSourceSettings.get("maxConnections")));
                }
                //if(dataSourceSettings.get("maxStatements") != null)
                //    config.getMaxActive(Integer.valueOf(dataSourceSettings.get("maxStatements")));
                //if (dataSourceSettings.get("schema") != null)
                //    config.setSchema(dataSourceSettings.get("schema"))
                var dataSource = new HikariDataSource(config);
                Jndi.bind(dataSourceName, dataSource);
                boundDataSources.add(dataSource);
                System.out.println("'" + dataSourceName + "' data source has been bound");
            } catch (NamingException ex) {
                throw new IllegalStateException(ex);
            }
        });
        if (!boundDataSources.isEmpty()) {
            getProject().getGradle().buildFinished(buildResult -> {
                boundDataSources.forEach(HikariDataSource::close);
                System.out.println(boundDataSources.size() > 1 ? boundDataSources.size() + " data sources have been unbound" : "Data source has been unbound");
            });
        }
    }
}
