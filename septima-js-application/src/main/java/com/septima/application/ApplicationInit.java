package com.septima.application;

import com.septima.application.jaspic.SeptimaAuthConfigProvider;
import com.septima.application.jaspic.SeptimaServerAuthConfig;

import javax.security.auth.message.config.AuthConfigFactory;
import javax.servlet.AsyncContext;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRegistration;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ApplicationInit {

    public static class SeptimaContextListener implements ServletContextListener {

        private volatile String authConfigRegistered;

        @Override
        public void contextInitialized(ServletContextEvent anEvent) {
            ServletContext context = anEvent.getServletContext();
            String jaspicModules = context.getInitParameter(SeptimaServerAuthConfig.JASPIC_LOGIN_MODULES_PARAMETER_NAME);
            if (jaspicModules != null && !jaspicModules.isBlank()) {
                Map<String, Object> parameters = new HashMap<>();
                Iterator<String> throughInitParameters = context.getInitParameterNames().asIterator();
                while (throughInitParameters.hasNext()) {
                    String initParamName = throughInitParameters.next();
                    parameters.put(initParamName, context.getInitParameter(initParamName));
                }
                authConfigRegistered = AuthConfigFactory.getFactory().registerConfigProvider(SeptimaAuthConfigProvider.class.getName(), parameters, SeptimaAuthConfigProvider.LAYER, null, "Septima JASPIC provider");
            }
        }

        @Override
        public void contextDestroyed(ServletContextEvent anEvent) {
            if (authConfigRegistered != null) {
                AuthConfigFactory.getFactory().removeRegistration(authConfigRegistered);
            }
        }
    }

    public static class EndlessRequestHandler extends HttpServlet {

        private static boolean initialized;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
            synchronized (EndlessRequestHandler.class) {
                if (!initialized) {
                    Config config = Config.parse(req.getServletContext());
                    AsyncContext ctx = req.startAsync();
                    ctx.setTimeout(-1);
                    Futures.init(ctx::start);
                    Data.init(config);
                    Scope.init(config);
                    initialized = true;
                }
            }
        }
    }

    public static class OnRequest extends SeptimaContextListener {

        @Override
        public void contextInitialized(ServletContextEvent anEvent) {
            super.contextInitialized(anEvent);
            ServletRegistration.Dynamic reg = anEvent.getServletContext().addServlet("initServlet", EndlessRequestHandler.class);
            reg.setAsyncSupported(true);
            reg.setLoadOnStartup(100000);
            reg.addMapping("/init");
        }

        @Override
        public void contextDestroyed(ServletContextEvent anEvent) {
            Scope.done();
            Data.done();
            Futures.done();
            super.contextDestroyed(anEvent);
        }
    }

    public static class OnContext extends SeptimaContextListener {

        @Override
        public void contextInitialized(ServletContextEvent anEvent) {
            super.contextInitialized(anEvent);
            init(Config.parse(anEvent.getServletContext()));
        }

        protected void init(Config config) {
            Futures.init(config);
            Data.init(config);
            Scope.init(config);
        }

        @Override
        public void contextDestroyed(ServletContextEvent anEvent) {
            Scope.done();
            Data.done();
            Futures.done();
            super.contextDestroyed(anEvent);
        }
    }
}
