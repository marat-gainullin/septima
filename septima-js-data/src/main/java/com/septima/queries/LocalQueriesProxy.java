package com.septima.client.queries;

import com.septima.client.StoredQueryFactory;
import com.septima.client.Databases;
import com.septima.cache.ActualCacheEntry;
import com.septima.cache.PlatypusFiles;
import com.septima.cache.SourceIndexer;
import com.septima.script.Scripts;
import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mg
 */
public class LocalQueriesProxy implements QueriesProxy<SqlQuery> {

    protected Map<String, ActualCacheEntry<SqlQuery>> entries = new ConcurrentHashMap<>();
    protected SourceIndexer indexer;
    protected StoredQueryFactory factory;
    protected Databases core;

    public LocalQueriesProxy(Databases aBasesProxy, SourceIndexer aIndexer) throws Exception {
        super();
        indexer = aIndexer;
        factory = new StoredQueryFactory(aBasesProxy, this, indexer);
        core = aBasesProxy;
    }

    @Override
    public SqlQuery getQuery(String aName, Scripts.Space aSpace, Consumer<SqlQuery> onSuccess, Consumer<Exception> onFailure) throws Exception {
        Callable<SqlQuery> doWork = () -> {
            SqlQuery query;
            if (aName != null) {
                Date cachedTimeStamp = null;
                ActualCacheEntry<SqlQuery> entry = entries.get(aName);
                if (entry != null) {
                    cachedTimeStamp = entry.getTimeStamp();
                }
                File file = indexer.nameToFile(aName);
                if (file != null) {
                    Date filesTimeStamp = new Date(file.lastModified());
                    if (cachedTimeStamp == null || filesTimeStamp.after(cachedTimeStamp)) {
                        if (file.getName().endsWith(PlatypusFiles.SQL_FILE_END)) {
                            query = factory.loadQuery(aName);
                        } else {
                            String nameMsg;
                            if (aName.isEmpty()) {
                                nameMsg = "empty name";
                            } else {
                                nameMsg = "name '" + aName + "'";
                            }
                            throw new IllegalStateException("Entity with " + nameMsg + " can't be constructed. Entities can be constructed with only platypus queries application elements.");
                        }
                        entries.put(aName, new ActualCacheEntry<>(query, filesTimeStamp));
                    } else {
                        assert entry != null : "Entity is not found neither in memory, nor in files";
                        query = entry.getValue();
                    }
                } else {
                    // Let's support in memory only (without underlying files) queries. Used in createEntity().
                    if (entry != null) {
                        query = entry.getValue();
                    } else {
                        query = null;
                    }
                }
            } else {
                query = null;
            }
            return query;
        };
        if (onSuccess != null) {
            aSpace.process(() -> {
                try {
                    SqlQuery query = doWork.call();
                    try {
                        onSuccess.accept(query);
                    } catch (Exception ex) {
                        Logger.getLogger(LocalQueriesProxy.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } catch (Exception ex) {
                    if (onFailure != null) {
                        onFailure.accept(ex);
                    }
                }
            });
            return null;
        } else {
            return doWork.call();
        }
    }

    @Override
    public SqlQuery getCachedQuery(String aName) {
        if (aName != null) {
            ActualCacheEntry<SqlQuery> entry = entries.get(aName);
            if (entry != null) {
                return entry.getValue();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public void putCachedQuery(String aName, SqlQuery aQuery) {
        entries.put(aName, new ActualCacheEntry<>(aQuery, new Date()));
    }

    public Databases getCore() {
        return core;
    }

    public void clearCachedQueries() {
        entries.clear();
    }

    public void clearCachedQuery(String aQueryName) {
        if (aQueryName != null) {
            entries.remove(aQueryName);
        }
    }
}
