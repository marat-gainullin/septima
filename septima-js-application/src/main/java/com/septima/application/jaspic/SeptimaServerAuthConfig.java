package com.septima.application.jaspic;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.message.AuthException;
import javax.security.auth.message.MessageInfo;
import javax.security.auth.message.config.ServerAuthConfig;
import javax.security.auth.message.config.ServerAuthContext;
import javax.security.auth.message.module.ServerAuthModule;

public class SeptimaServerAuthConfig implements ServerAuthConfig {

    public static final String JASPIC_LOGIN_MODULES_PARAMETER_NAME = "jaspic.login.modules";

    private final String layer;
    private final String appContext;
    private final CallbackHandler handler;
    private final Map<String, String> properties;

    private volatile ServerAuthContext authContext;

    public SeptimaServerAuthConfig(String layer, String appContext, CallbackHandler handler,
                                   Map<String, String> properties) {
        this.layer = layer;
        this.appContext = appContext;
        this.handler = handler;
        this.properties = properties;
    }

    @Override
    public String getMessageLayer() {
        return layer;
    }

    @Override
    public String getAppContext() {
        return appContext;
    }

    @Override
    public String getAuthContextID(MessageInfo messageInfo) {
        return messageInfo.toString();
    }

    @Override
    public void refresh() {
        authContext = null;
    }

    @Override
    public boolean isProtected() {
        return false;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public ServerAuthContext getAuthContext(String authContextId, Subject serviceSubject,
                                            Map properties) throws AuthException {
        try {
            ServerAuthContext authContext = this.authContext;
            if (authContext == null) {
                synchronized (this) {
                    if (this.authContext == null) {
                        Map<String, String> mergedProperties = new HashMap<>();
                        if (this.properties != null) {
                            mergedProperties.putAll(this.properties);
                        }
                        if (properties != null) {
                            mergedProperties.putAll(properties);
                        }
                        String loginModules = mergedProperties.get(JASPIC_LOGIN_MODULES_PARAMETER_NAME);
                        if (loginModules != null && !loginModules.isBlank()) {
                            String[] modulesClassNames = loginModules.split("[| ,;]");
                            List<ServerAuthModule> modules = new ArrayList<>(modulesClassNames.length);
                            for (String className : modulesClassNames) {
                                Class<?> moduleClass = Class.forName(className);
                                ServerAuthModule module = (ServerAuthModule) moduleClass.getDeclaredConstructor().newInstance();
                                module.initialize(null, null, handler, mergedProperties);
                                modules.add(module);
                            }
                            this.authContext = new SeptimaServerAuthContext(modules);
                        } else {
                            throw new AuthException("Mandatory property '" + JASPIC_LOGIN_MODULES_PARAMETER_NAME + "' has not been found in JASPIC config");
                        }
                    }
                    authContext = this.authContext;
                }
            }
            return authContext;
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException ex) {
            throw new AuthException(ex.getMessage());
        }
    }
}
