/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.queries;

import com.septima.client.Databases;
import com.septima.client.resourcepool.BearResourcePool;
import com.septima.client.resourcepool.GeneralResourceProvider;
import com.septima.client.settings.DbConnectionSettings;
import com.septima.util.IdGenerator;

/**
 *
 * @author mg
 */
public class DatabasesWithResource implements AutoCloseable {

    protected String resourceName;
    protected Databases client;

    public DatabasesWithResource(DbConnectionSettings aSettings) throws Exception {
        super();
        resourceName = "TestDb-" + IdGenerator.genStringId();
        GeneralResourceProvider.getInstance().registerDatasource(resourceName, aSettings);
        client = new Databases(resourceName, true, BearResourcePool.DEFAULT_MAXIMUM_SIZE);
    }

    public Databases getClient() {
        return client;
    }

    @Override
    public void close() throws Exception {
        GeneralResourceProvider.getInstance().unregisterDatasource(resourceName);
    }

}
