package com.septima.application.jaspic;

import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.message.AuthException;
import javax.security.auth.message.config.AuthConfigFactory;
import javax.security.auth.message.config.AuthConfigProvider;
import javax.security.auth.message.config.ClientAuthConfig;
import javax.security.auth.message.config.ServerAuthConfig;

public class SeptimaAuthConfigProvider implements AuthConfigProvider {

    public static final String LAYER = "HttpServlet";

    private final Map<String, String> properties;

    private volatile ServerAuthConfig authConfig;

    public SeptimaAuthConfigProvider(Map<String, String> properties, AuthConfigFactory factory) {
        this.properties = properties;
        if (factory != null) {
            factory.registerConfigProvider(this, LAYER, null, "Self registration");
        }
    }

    @Override
    public ClientAuthConfig getClientAuthConfig(String layer, String appContext,
                                                CallbackHandler handler) throws AuthException {
        return null;
    }

    @Override
    public ServerAuthConfig getServerAuthConfig(String layer, String appContext,
                                                CallbackHandler handler) throws AuthException {
        ServerAuthConfig serverAuthConfig = this.authConfig;
        if (serverAuthConfig == null) {
            synchronized (this) {
                if (this.authConfig == null) {
                    this.authConfig = new SeptimaServerAuthConfig(layer, appContext, handler, properties);
                }
                serverAuthConfig = this.authConfig;
            }
        }
        return serverAuthConfig;
    }

    @Override
    public void refresh() {
        ServerAuthConfig serverAuthConfig = this.authConfig;
        if (serverAuthConfig != null) {
            serverAuthConfig.refresh();
        }
    }
}
