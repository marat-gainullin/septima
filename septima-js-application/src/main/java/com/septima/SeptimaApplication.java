/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima;

import com.septima.cache.ScriptDocument;
import com.septima.cache.ScriptDocuments;
import com.septima.client.DatabasesClient;
import com.septima.client.SqlQuery;
import com.septima.cache.ScriptDocument.ModuleDocument;
import com.septima.client.queries.ContextHost;
import com.septima.client.queries.QueriesProxy;
import com.septima.client.scripts.ScriptedResource;
import com.septima.script.HasPublished;
import com.septima.script.JsDoc;
import com.septima.script.Scripts;
import com.septima.handlers.RPCRequestHandler;
import com.septima.script.JsObjectException;
import com.septima.script.SystemJSCallback;
import java.io.File;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.nashorn.api.scripting.JSObject;
import jdk.nashorn.internal.runtime.Undefined;

/**
 * The core class for platypus server infrastructure (e.g. Standalone J2SE
 * server and J2EE servlets).
 *
 * @author mg
 */
public abstract class SeptimaApplication implements ContextHost, Application<SqlQuery> {

    protected String startModuleName;
    protected final Scripts.Space[] statelessSpaces;
    protected Sessions sessions;
    protected ScriptedDatabasesClient basesProxy;
    protected Indexer indexer;
    protected ModulesIndexer modules;
    protected QueriesProxy<SqlQuery> queries;
    protected ScriptDocuments scriptDocuments;
    protected FormsDocuments forms = new FormsDocuments();
    protected ReportsConfigs reports = new ReportsConfigs();
    protected ModelsDocuments models = new ModelsDocuments();
    protected ServerModulesProxy localServerModules = new LpcProxy(this);
    protected Scripts.Space queueSpace;

    public SeptimaApplication(Indexer aIndexer, ModulesIndexer aModules, QueriesProxy<SqlQuery> aQueries, ScriptedDatabasesClient aDatabasesClient, ScriptDocuments aSecurityConfigs, String aDefaultAppElement) throws Exception {
        this(aIndexer, aModules, aQueries, aDatabasesClient, aSecurityConfigs, aDefaultAppElement, new Sessions(), (Runtime.getRuntime().availableProcessors() + 1) * 10);
    }

    public SeptimaApplication(Indexer aIndexer, ModulesIndexer aModules, QueriesProxy<SqlQuery> aQueries, ScriptedDatabasesClient aDatabasesClient, ScriptDocuments aSecurityConfigs, String aDefaultAppElement, Sessions aSessions, int aMaxStatelessSpaces) throws Exception {
        super();
        indexer = aIndexer;
        modules = aModules;
        queries = aQueries;
        basesProxy = aDatabasesClient;
        sessions = aSessions;
        startModuleName = aDefaultAppElement;
        scriptDocuments = aSecurityConfigs;
        queueSpace = Scripts.createQueue();
        statelessSpaces = new Scripts.Space[Math.max(1, aMaxStatelessSpaces)];
        for (int s = 0; s < statelessSpaces.length; s++) {
            statelessSpaces[s] = Scripts.createSpace();
        }
    }

    public Scripts.Space getQueueSpace() {
        return queueSpace;
    }

    public Indexer getIndexer() {
        return indexer;
    }

    @Override
    public ModulesIndexer getModules() {
        return modules;
    }

    @Override
    public QueriesProxy<SqlQuery> getQueries() {
        return queries;
    }

    public ScriptDocuments getScriptDocuments() {
        return scriptDocuments;
    }

    @Override
    public ModelsDocuments getModels() {
        return models;
    }

    @Override
    public ReportsConfigs getReports() {
        return reports;
    }

    @Override
    public FormsDocuments getForms() {
        return forms;
    }

    @Override
    public ServerModulesProxy getServerModules() {
        return localServerModules;
    }

    public Sessions getSessions() {
        return sessions;
    }

    public DatabasesClient getDatabases() {
        return basesProxy;
    }

    public String getStartModuleName() {
        return startModuleName;
    }

    public int getMaxStatelessSpaces() {
        return statelessSpaces.length;
    }

    /**
     * Executes a script module according to all rules defimed within
     * Platypus.js Such as @stateless and @rezident annotations, async-io
     * convensions etc.
     *
     * @param aModuleName
     * @param aMethodName
     * @param aArguments
     * @param aNetworkRPC
     * @param aOnSuccess
     * @param aOnFailure
     */
    public void executeMethod(String aModuleName, String aMethodName, Object[] aArguments, boolean aNetworkRPC, Consumer<Object> aOnSuccess, Consumer<Exception> aOnFailure) {
        Scripts.LocalContext callingContext = Scripts.getContext();
        Scripts.Space callingSpace = Scripts.getSpace();
        Object[] copiedArguments = makeArgumentsCopy(callingSpace, aArguments);
        Consumer<Object> onSuccess = (Object res) -> {
            if (aOnSuccess != null) {
                Scripts.Space targetSpace = Scripts.getSpace();
                Object copiedRes = targetSpace.makeCopy(res);
                callingSpace.process(callingContext, () -> {
                    assert Scripts.getSpace() == callingSpace;
                    aOnSuccess.accept(callingSpace.restoreCopy(copiedRes));
                });
            }
        };
        Consumer<Exception> onFailure = (Exception ex) -> {
            if (aOnFailure != null) {
                Scripts.Space targetSpace = Scripts.getSpace();
                Exception copiedEx = ex instanceof JsObjectException ? new JsObjectException(targetSpace.makeCopy(((JsObjectException) ex).getData())) : ex;
                callingSpace.process(callingContext, () -> {
                    assert Scripts.getSpace() == callingSpace;
                    Exception restoredEx = copiedEx instanceof JsObjectException ? new JsObjectException(callingSpace.restoreCopy(((JsObjectException) copiedEx).getData())) : copiedEx;
                    aOnFailure.accept(restoredEx);
                });
            }
        };
        if (aModuleName == null || aModuleName.isEmpty()) {
            onFailure.accept(new Exception("Module name is missing. Unnamed server modules are not allowed."));
        } else if (aMethodName == null || aMethodName.isEmpty()) {
            onFailure.accept(new Exception("Module's method name is missing."));
        } else {
            Consumer<ModuleDocument> withConfig = (ModuleDocument config) -> {
                try {
                    if (!aNetworkRPC || config.hasAnnotation(JsDoc.Tag.PUBLIC_TAG)) {
                        // Let's perform security checks
                        ServerModuleStructureRequestHandler.checkPrincipalPermission(config.getAllowedRoles(), aModuleName);
                        Scripts.Space targetSpace;
                        Session targetSession;
                        if (config.hasAnnotation(JsDoc.Tag.RESIDENT_TAG)) {
                            targetSession = sessions.getSystemSession();
                            targetSpace = targetSession.getSpace();
                        } else if (config.hasAnnotation(JsDoc.Tag.STATELESS_TAG)) {
                            targetSession = null;
                            int rnd = new Random().nextInt(statelessSpaces.length);
                            targetSpace = statelessSpaces[rnd];
                        } else {// Statefull session module
                            targetSession = (Session) callingContext.getSession();
                            targetSpace = targetSession.getSpace();
                        }
                        Scripts.LocalContext targetContext = new Scripts.LocalContext(callingContext.getRequest(), callingContext.getResponse(), callingContext.getPrincipal(), callingContext.getSession());
                        targetSpace.process(targetContext, () -> {
                            assert Scripts.getSpace() == targetSpace;
                            try {
                                Consumer<JSObject> withModuleConstructor = (JSObject constr) -> {
                                    assert Scripts.getSpace() == targetSpace;
                                    try {
                                        JSObject moduleInstance;
                                        if (targetSession == null || !targetSession.containsModule(aModuleName)) {
                                            if (constr != null) {
                                                moduleInstance = (JSObject) constr.newObject(new Object[]{});
                                                if (targetSession != null) {
                                                    targetSession.registerModule(aModuleName, moduleInstance);
                                                }
                                            } else {
                                                throw new IllegalArgumentException(String.format(RPCRequestHandler.MODULE_MISSING_OR_NOT_A_MODULE, aModuleName));
                                            }
                                        } else {
                                            moduleInstance = targetSession.getModule(aModuleName);
                                        }
                                        if (moduleInstance != null) {
                                            Logger.getLogger(SeptimaApplication.class.getName()).log(Level.FINE, RPCRequestHandler.EXECUTING_METHOD_TRACE_MSG, new Object[]{aMethodName, aModuleName});
                                            Object oFun = moduleInstance.getMember(aMethodName);
                                            if (oFun instanceof JSObject && ((JSObject) oFun).isFunction()) {
                                                AtomicBoolean executed = new AtomicBoolean();
                                                List<Object> arguments = new ArrayList<>();
                                                for (Object argument : copiedArguments) {
                                                    arguments.add(targetSpace.restoreCopy(argument));
                                                }
                                                arguments.add(new SystemJSCallback() {
                                                    @Override
                                                    public Object call(final Object thiz, final Object... largs) {
                                                        if (!aNetworkRPC || !executed.get()) {
                                                            executed.set(true);
                                                            Object returned = largs.length > 0 ? largs[0] : null;
                                                            onSuccess.accept(returned);// WARNING! Don't insert .toJava() because of RPC handler
                                                        } else {
                                                            Logger.getLogger(RPCRequestHandler.class.getName()).log(Level.WARNING, RPCRequestHandler.BOTH_IO_MODELS_MSG, new Object[]{aMethodName, aModuleName});
                                                        }
                                                        return null;
                                                    }

                                                });
                                                arguments.add(new SystemJSCallback() {
                                                    @Override
                                                    public Object call(final Object thiz, final Object... largs) {
                                                        if (!aNetworkRPC || !executed.get()) {
                                                            executed.set(true);
                                                            Object reason = largs.length > 0 ? largs[0] : null;
                                                            if (reason instanceof Exception) {
                                                                onFailure.accept((Exception) reason);// WARNING! Don't insert .toJava() because of RPC handler
                                                            } else {
                                                                onFailure.accept(new JsObjectException(reason));
                                                            }
                                                        } else {
                                                            Logger.getLogger(RPCRequestHandler.class.getName()).log(Level.WARNING, RPCRequestHandler.BOTH_IO_MODELS_MSG, new Object[]{aMethodName, aModuleName});
                                                        }
                                                        return null;
                                                    }

                                                });
                                                Scripts.getContext().initAsyncs(0);
                                                try {
                                                    ServerModuleStructureRequestHandler.checkPrincipalPermission(config.getPropertyAllowedRoles().get(aMethodName), aModuleName + "." + aMethodName);
                                                    Object result = ((JSObject) oFun).call(moduleInstance, arguments.toArray());
                                                    int asyncs = Scripts.getContext().getAsyncsCount();
                                                    if (!(result instanceof Undefined) || asyncs == 0) {
                                                        if (!executed.get()) {
                                                            executed.set(true);
                                                            onSuccess.accept(result);// WARNING! Don't insert .toJava() because of RPC handler
                                                        } else {
                                                            Logger.getLogger(RPCRequestHandler.class.getName()).log(Level.WARNING, RPCRequestHandler.BOTH_IO_MODELS_MSG, new Object[]{aMethodName, aModuleName});
                                                        }
                                                    }
                                                } finally {
                                                    Scripts.getContext().initAsyncs(null);
                                                }
                                            } else {
                                                throw new Exception(String.format(RPCRequestHandler.METHOD_MISSING_MSG, aMethodName, aModuleName));
                                            }
                                        } else {
                                            throw new Exception(String.format(RPCRequestHandler.MODULE_MISSING_MSG, aModuleName));
                                        }
                                    } catch (Exception ex) {
                                        onFailure.accept(ex);
                                    }
                                };
                                JSObject moduleConstructor = targetSpace.lookup(aModuleName);
                                if (moduleConstructor != null) {
                                    withModuleConstructor.accept(moduleConstructor);
                                } else {
                                    ScriptedResource._require(new String[]{aModuleName}, null, targetSpace, new HashSet<>(), (Void v) -> {
                                        assert Scripts.getSpace() == targetSpace;
                                        assert Scripts.getContext() == targetContext;
                                        withModuleConstructor.accept(targetSpace.lookup(aModuleName));
                                    }, (Exception ex) -> {
                                        onFailure.accept(ex);
                                    });
                                }
                            } catch (Exception ex) {
                                onFailure.accept(ex);
                            }
                        });
                    } else {
                        throw new AccessControlException(String.format("Public access to module %s is denied.", aModuleName));//NOI18N
                    }
                } catch (AccessControlException ex) {
                    onFailure.accept(ex);
                }
            };
            try {
                ScriptDocument.ModuleDocument moduleDoc = lookupModuleDocument(aModuleName);
                if (moduleDoc != null) {
                    withConfig.accept(moduleDoc);
                } else {
                    throw new IllegalArgumentException(String.format(RPCRequestHandler.MODULE_MISSING_OR_NOT_A_MODULE, aModuleName));
                }
            } catch (Exception ex) {
                onFailure.accept(ex);
            }
        }
    }

    public ModuleDocument lookupModuleDocument(String aModuleName) throws Exception {
        File indexedFile = indexer.nameToFile(aModuleName);
        if (indexedFile != null && indexedFile.getName().endsWith(SeptimaFiles.JAVASCRIPT_FILE_END)) {
            String defaultModuleName = indexer.getDefaultModuleName(indexedFile);
            ScriptDocument scriptDoc = scriptDocuments.getCachedConfig(defaultModuleName);
            if (scriptDoc == null) {
                scriptDoc = scriptDocuments.get(defaultModuleName, indexedFile);
            }
            if (scriptDoc != null) {
                return scriptDoc.getModules().get(aModuleName);
            } else {
                throw new IllegalArgumentException(String.format(RPCRequestHandler.MODULE_MISSING_OR_NOT_A_MODULE, aModuleName));
            }
        } else {
            throw new IllegalArgumentException(String.format(RPCRequestHandler.MODULE_MISSING_OR_NOT_A_MODULE, aModuleName));
        }
    }

    public Object[] makeArgumentsCopy(Scripts.Space aSpace, Object[] aArguments) {
        Object[] arguments = Arrays.copyOf(aArguments, aArguments.length);
        for (int a = 0; a < arguments.length; a++) {
            if (arguments[a] instanceof HasPublished) {
                arguments[a] = ((HasPublished) arguments[a]).getPublished();
            } else {
                arguments[a] = aSpace.makeCopy(arguments[a]);
            }
        }
        return arguments;
    }

    @Override
    public String preparationContext() throws Exception {
        Scripts.LocalContext context = Scripts.getContext();
        if (context != null && context.getPrincipal() != null) {
            return ((PlatypusPrincipal) context.getPrincipal()).getContext();
        } else {
            return null;
        }
    }

    @Override
    public String unpreparationContext() throws Exception {
        return basesProxy.getMetadataCache(null).getDatasourceSchema();
    }
}
