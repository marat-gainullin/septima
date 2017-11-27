package com.eas.client;

import com.eas.client.cache.FormsDocuments;
import com.eas.client.cache.ModelsDocuments;
import com.eas.client.cache.ReportsConfigs;
import com.eas.client.cache.ScriptsConfigs;
import com.eas.client.queries.QueriesProxy;
import com.eas.client.queries.Query;

/**
 *
 * @author mg
 * @param <Q>
 */
public interface Application<Q extends Query> {

    public QueriesProxy<Q> getQueries();

    public ModulesProxy getModules();

    public ServerModulesProxy getServerModules();

    public ModelsDocuments getModels();

    public ReportsConfigs getReports();

    public ScriptsConfigs getScriptsConfigs();
}
