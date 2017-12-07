package com.septima;

import com.septima.script.AlreadyPublishedException;
import com.septima.script.HasPublished;
import com.septima.script.Scripts;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import jdk.nashorn.api.scripting.JSObject;
import jdk.nashorn.internal.runtime.JSType;

/**
 * A client session
 *
 * <p>
 * This object is created to represent a session with successfully authenticated
 * client. It is used to associate various resources such as tasks with a
 * client. Whenever a session is <code>cleanup()</code>-ed, the resources are
 * deleted.</p> Method rollback of database client is also invoked.
 *
 * @author pk, mg refactoring
 */
public class Session implements HasPublished {

    protected JSObject published;
    //
    private final String sessionId;
    private final Map<String, JSObject> modules = new HashMap<>();
    private Scripts.Space space;
    private Principal principal;

    /**
     * Creates a new session with given session id.
     *
     * @param aSessionId unique session id.
     */
    public Session(String aSessionId) {
        super();
        sessionId = aSessionId;
    }

    public Scripts.Space getSpace() {
        return space;
    }

    public void setSpace(Scripts.Space aValue) {
        space = aValue;
    }

    public void setPrincipal(Principal aValue) {
        principal = aValue;
    }

    /**
     * Deletes all resources belonging to this session.
     */
    public void cleanup() {
        // server modules
        modules.clear();
        // data in client's transaction
    }

    /**
     * Returns server module by name.
     *
     * @param aName
     * @return
     */
    public JSObject getModule(String aName) {
        return modules.get(aName);
    }

    public boolean containsModule(String aName) {
        return modules.containsKey(aName);
    }

    public void registerModule(String aName, JSObject aModule) {
        if (aName == null || aName.isEmpty()) {
            JSObject c = (JSObject) aModule.getMember("constructor");
            aName = JSType.toString(c.getMember("name"));
        }
        modules.put(aName, aModule);
    }

    public void unregisterModule(String aModuleName) {
        modules.remove(aModuleName);
    }

    public void unregisterModules() {
        modules.clear();
    }

    public Set<Map.Entry<String, JSObject>> getModulesEntries() {
        return Collections.unmodifiableSet(modules.entrySet());
    }

    /**
     * Returns this session's id.
     *
     * @return session id.
     */
    public String getId() {
        return sessionId;
    }

    public boolean isNew() {
        return false;
    }

    @Override
    public void setPublished(JSObject aValue) {
        if (published != null) {
            throw new AlreadyPublishedException();
        }
        published = aValue;
    }

    @Override
    public JSObject getPublished() {
        return published;
    }
}
