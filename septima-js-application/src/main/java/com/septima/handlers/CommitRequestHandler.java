package com.septima.handlers;

import com.septima.RequestHandler;
import com.septima.changes.*;
import com.septima.client.DatabasesClient;
import com.septima.client.SqlCompiledQuery;
import com.septima.client.SqlQuery;
import com.septima.login.AnonymousPrincipal;
import com.septima.client.threetier.requests.CommitRequest;
import com.septima.script.Scripts;
import com.septima.SeptimaApplication;
import com.septima.Session;
import jdk.nashorn.api.scripting.JSObject;
import jdk.nashorn.internal.runtime.JSType;

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.AuthPermission;

/**
 *
 * @author mg
 */
public class CommitRequestHandler extends RequestHandler<CommitRequest, CommitRequest.Response> {

    private static final class ChangesJSONReader implements TransferableChangeVisitor {

        private static final String CHANGE_DATA_NAME = "data";
        private static final String CHANGE_KEYS_NAME = "keys";
        private static final String CHANGE_PARAMETERS_NAME = "parameters";
        protected JSObject sChange;
        protected String entityName;
        protected Scripts.Space space;

        public ChangesJSONReader(JSObject aSChange, String aEntityName, Scripts.Space aSpace) throws Exception {
            super();
            sChange = aSChange;
            entityName = aEntityName;
            space = aSpace;
        }

        protected List<NamedValue> parseObjectProperties(Object oData) throws Exception {
            List<NamedValue> data = new ArrayList<>();
            if (oData instanceof JSObject) {
                JSObject sValue = (JSObject) oData;
                sValue.keySet().stream().forEach((sValueName) -> {
                    Object oValueValue = sValue.getMember(sValueName);
                    Object convertedValueValue = space.toJava(oValueValue);
                    data.add(new NamedValue(sValueName, convertedValueValue));
                });
            }
            return data;
        }

        @Override
        public void visit(Insert aChange) throws Exception {
            Object oData = sChange.getMember(CHANGE_DATA_NAME);
            aChange.getData().addAll(parseObjectProperties(oData));
        }

        @Override
        public void visit(Update aChange) throws Exception {
            Object oData = sChange.getMember(CHANGE_DATA_NAME);
            aChange.getData().addAll(parseObjectProperties(oData));
            Object oKeys = sChange.getMember(CHANGE_KEYS_NAME);
            aChange.getKeys().addAll(parseObjectProperties(oKeys));
        }

        @Override
        public void visit(Delete aChange) throws Exception {
            Object oKeys = sChange.getMember(CHANGE_KEYS_NAME);
            aChange.getKeys().addAll(parseObjectProperties(oKeys));
        }

        @Override
        public void visit(CommandRequest aRequest) throws Exception {
            Object oParameters = sChange.getMember(CHANGE_PARAMETERS_NAME);
            List<NamedValue> values = parseObjectProperties(oParameters);
            values.stream().forEach(cv -> aRequest.getParameters().put(cv.name, cv));
        }

        public static List<Change.Transferable> read(String aChangesJson, Scripts.Space aSpace) throws Exception {
            List<Change.Transferable> changes = new ArrayList<>();
            Object sChanges = aSpace.parseJsonWithDates(aChangesJson);
            if (sChanges instanceof JSObject) {
                JSObject jsChanges = (JSObject) sChanges;
                int length = JSType.toInteger(jsChanges.getMember("length"));
                for (int i = 0; i < length; i++) {
                    Object oChange = jsChanges.getSlot(i);
                    if (oChange instanceof JSObject) {
                        JSObject sChange = (JSObject) oChange;
                        if (sChange.hasMember("kind") && sChange.hasMember("entity")) {
                            String sKind = JSType.toString(sChange.getMember("kind"));
                            String sEntityName = JSType.toString(sChange.getMember("entity"));
                            ChangesJSONReader reader = new ChangesJSONReader(sChange, sEntityName, aSpace);
                            Change.Transferable change = null;
                            switch (sKind) {
                                case "insert":
                                    change = new Insert(sEntityName);
                                    change.accept(reader);
                                    break;
                                case "update":
                                    change = new Update(sEntityName);
                                    change.accept(reader);
                                    break;
                                case "delete":
                                    change = new Delete(sEntityName);
                                    change.accept(reader);
                                    break;
                                case "command":
                                    change = new CommandRequest(sEntityName);
                                    change.accept(reader);
                                    break;
                            }
                            if (change != null) {
                                changes.add(change);
                            } else {
                                Logger.getLogger(ChangesJSONReader.class.getName()).log(Level.SEVERE, String.format("Unknown type of change occured %s.", sKind));
                            }
                        } else {
                            Logger.getLogger(ChangesJSONReader.class.getName()).log(Level.SEVERE, "Kind and entity of change both must present");
                        }
                    } else {
                        Logger.getLogger(ChangesJSONReader.class.getName()).log(Level.SEVERE, "Every change must be an object.");
                    }
                }
            } else {
                Logger.getLogger(ChangesJSONReader.class.getName()).log(Level.SEVERE, "Changes must be an array.");
            }
            return changes;
        }

    }
    private static final class ChangesSortProcess {

        private final List<Change.Applicable> expectedChanges = new ArrayList<>();
        private final String defaultDatasource;
        private int factCalls;
        private final Consumer<Map<String, List<Change.Applicable>>> onSuccess;
        private final Consumer<Exception> onFailure;

        private final List<AccessControlException> accessDeniedEntities = new ArrayList<>();
        private final List<Exception> notRetrievedEntities = new ArrayList<>();
        private final Map<String, String> datasourcesOfEntities = new HashMap();

        public ChangesSortProcess(String aDefaultDatasource, Consumer<Map<String, List<Change.Applicable>>> aOnSuccess, Consumer<Exception> aOnFailure) {
            super();
            defaultDatasource = aDefaultDatasource;
            onSuccess = aOnSuccess;
            onFailure = aOnFailure;
        }

        public void datasourceDescovered(String aEntityName, String aDatasourceName) {
            datasourcesOfEntities.put(aEntityName, aDatasourceName);
        }

        protected String assembleErrors() {
            if (!notRetrievedEntities.isEmpty() || !accessDeniedEntities.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                Consumer<Exception> appender = (ex) -> {
                    if (sb.length() > 0) {
                        sb.append("\n");
                    }
                    sb.append(ex.getMessage());
                };
                accessDeniedEntities.stream().forEach(appender);
                notRetrievedEntities.stream().forEach(appender);
                return sb.toString();
            } else {
                return "Unknown error";
            }
        }

        public void complete(Change.Applicable aChange, AccessControlException accessDenied, Exception failed) {
            expectedChanges.add(aChange);
            if (accessDenied != null) {
                accessDeniedEntities.add(accessDenied);
            }
            if (failed != null) {
                notRetrievedEntities.add(failed);
            }
            if (++factCalls == expectedChanges.size()) {
                if (accessDeniedEntities.isEmpty() && notRetrievedEntities.isEmpty()) {
                    if (onSuccess != null) {
                        Map<String, List<Change.Applicable>> changeLogs = new HashMap<>();
                        expectedChanges.stream().forEach((Change.Applicable aSortedChange) -> {
                            String datasourceName = datasourcesOfEntities.get(aSortedChange.getEntity());
                            // defaultDatasource is needed here to avoid multi transaction
                            // actions against the same datasource, leading to unexpected
                            // row level locking and deadlocks in two phase transaction commit process
                            if (datasourceName == null || datasourceName.isEmpty()) {
                                datasourceName = defaultDatasource;
                            }
                            List<Change.Applicable> targetChangeLog = changeLogs.get(datasourceName);
                            if (targetChangeLog == null) {
                                targetChangeLog = new ArrayList<>();
                                changeLogs.put(datasourceName, targetChangeLog);
                            }
                            targetChangeLog.add(aSortedChange);
                        });
                        onSuccess.accept(changeLogs);
                    }
                } else {
                    if (onFailure != null) {
                        onFailure.accept(new IllegalStateException(assembleErrors()));
                    }
                }
            }
        }
    }

    public CommitRequestHandler(SeptimaApplication aServerCore, CommitRequest aRequest) {
        super(aServerCore, aRequest);
    }

    private AccessControlException checkWritePrincipalPermission(PlatypusPrincipal aPrincipal, String aEntityName, Set<String> writeRoles) {
        if (writeRoles != null && !writeRoles.isEmpty()
                && (aPrincipal == null || !aPrincipal.hasAnyRole(writeRoles))) {
            return new AccessControlException(String.format("Access denied for write (entity: %s) for '%s'.", aEntityName != null ? aEntityName : "", aPrincipal != null ? aPrincipal.getName() : null), aPrincipal instanceof AnonymousPrincipal ? new AuthPermission("*") : null);
        } else {
            return null;
        }
    }

    @Override
    public void handle(Session aSession, Consumer<CommitRequest.Response> onSuccess, Consumer<Exception> onFailure) {
        try {
            List<Change.Transferable> changes = ChangesJSONReader.read(getRequest().getChangesJson(), Scripts.getSpace());
            DatabasesClient client = getServerCore().getDatabases();
            Map<String, SqlCompiledQuery> compiledEntities = new HashMap<>();

            ChangesSortProcess process = new ChangesSortProcess(client.getDefaultDatasourceName(), (Map<String, List<Change.Applicable>> changeLogs) -> {
                try {
                    client.commit(changeLogs, (Integer aUpdated) -> {
                        if (onSuccess != null) {
                            onSuccess.accept(new CommitRequest.Response(aUpdated));
                        }
                    }, onFailure);
                } catch (Exception ex) {
                    Logger.getLogger(CommitRequestHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }, onFailure);
            if (changes.isEmpty()) {
                if (onSuccess != null) {
                    onSuccess.accept(new CommitRequest.Response(0));
                }
            } else {
                changes.stream().forEach((change) -> {
                    try {
                        ((LocalQueriesProxy) serverCore.getQueries()).getQuery(change.getEntity(), Scripts.getSpace(), (SqlQuery query) -> {
                            if (query != null) {
                                process.datasourceDescovered(change.getEntity(), query.getDatasourceName());
                                if (query.isPublicAccess()) {
                                    AccessControlException accessControlEx = checkWritePrincipalPermission((PlatypusPrincipal) Scripts.getContext().getPrincipal(), change.getEntity(), query.getWriteRoles());
                                    if (accessControlEx != null) {
                                        process.complete(null, accessControlEx, null);
                                    } else {
                                        try {
                                            if (change instanceof CommandRequest) {
                                                CommandRequest commandRequest = (CommandRequest)change;
                                                SqlCompiledQuery compiled = compiledEntities.computeIfAbsent(change.getEntity(), en -> {
                                                    try {
                                                        return query.compile();
                                                    } catch (Exception ex) {
                                                        throw new IllegalStateException(ex);
                                                    }
                                                });
                                                process.complete(compiled.prepareCommand(commandRequest.getParameters()), null, null);
                                            } else {
                                                process.complete((Change.Applicable)change, null, null);
                                            }
                                        } catch (Exception ex) {
                                            Logger.getLogger(CommitRequestHandler.class.getName()).log(Level.SEVERE, null, ex);
                                            process.complete(null, null, ex);
                                        }
                                    }
                                } else {
                                    process.complete(null, new AccessControlException(String.format("Public access to entity '%s' is denied while commiting changes for it.", change.getEntity())), null);
                                }
                            } else {
                                process.complete(null, null, new IllegalArgumentException(String.format("Entity '%s' is not found", change.getEntity())));
                            }
                        }, (Exception ex) -> {
                            process.complete(null, null, ex);
                        });
                    } catch (Exception ex) {
                        Logger.getLogger(CommitRequestHandler.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });
            }
        } catch (Exception ex) {
            onFailure.accept(ex);
        }
    }

}
