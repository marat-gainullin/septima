package com.eas.client;

import com.eas.script.Scripts;
import java.util.function.Consumer;
import jdk.nashorn.api.scripting.JSObject;

/**
 *
 * @author mg
 */
public interface ServerModulesProxy {

    public abstract ServerModuleInfo getCachedStructure(String aName) throws Exception;

    public abstract ServerModuleInfo getServerModuleStructure(String aName, Scripts.Space aSpace, Consumer<ServerModuleInfo> onSuccess, Consumer<Exception> onFailure) throws Exception;

    public abstract Object callServerModuleMethod(String aModuleName, String aMethodName, Scripts.Space aSpace, JSObject onSuccess, JSObject onFailure, Object... aArguments) throws Exception;
}
