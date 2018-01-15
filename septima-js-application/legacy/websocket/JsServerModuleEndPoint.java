/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.websocket;

import com.septima.client.ClientConstants;
import com.septima.client.DatabasesClient;
import com.septima.login.AnonymousPrincipal;
import com.septima.script.Scripts;
import com.septima.SeptimaApplication;
import com.septima.Sessions;
import com.septima.http.SeptimaServlet;
import com.septima.util.IdGenerator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.ScriptException;
import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import jdk.nashorn.api.scripting.JSObject;

/**
 *
 * @author mg
 */
@ServerEndpoint(value = "/{module-name}", configurator = JsServerModuleEndPointConfigurator.class)
public class JsServerModuleEndPoint {

    //
    private static final String WS_ON_OPEN = "onopen";
    private static final String WS_ON_MESSAGE = "onmessage";
    private static final String WS_ON_CLOSE = "onclose";
    private static final String WS_ON_ERROR = "onerror";

    protected volatile String wasPlatypusSessionId;
    protected volatile String moduleName;

    public JsServerModuleEndPoint() {
        super();
    }

    protected void in(SeptimaApplication platypusCore, Session websocketSession, Consumer<com.septima.Session> aHandler) throws Exception {
        HandshakeRequest handshake = (HandshakeRequest) websocketSession.getUserProperties().get(JsServerModuleEndPointConfigurator.HANDSHAKE_REQUEST);
        String userName = websocketSession.getUserPrincipal() != null ? websocketSession.getUserPrincipal().getName() : null;
        Consumer<com.septima.Session> withPlatypusSession = (com.septima.Session aSession) -> {
            // Websocket executor thread or sessions accounting thread
            Scripts.LocalContext context = new Scripts.LocalContext(platypusPrincipal(handshake, websocketSession), aSession);
            aSession.getSpace().process(context, () -> {
                // temporarily session thread 
                try {
                    aHandler.accept(aSession);
                } catch (Exception ex) {
                    Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        };
        com.septima.Session lookedup0 = platypusSessionByWebSocketSession(websocketSession);
        if (lookedup0 == null) {// Zombie check
            platypusCore.getQueueSpace().process(() -> {
                // sessions accounting thread
                com.septima.Session lookedup1 = platypusSessionByWebSocketSession(websocketSession);
                if (lookedup1 == null) {// Zombie check
                    try {
                        Consumer<String> withDataContext = (String dataContext) -> {
                            // still sessions accounting thread
                            String platypusSessionId = (String) websocketSession.getUserProperties().get(SeptimaServlet.PLATYPUS_SESSION_ID_ATTR_NAME);
                            // platypusSessionId may be replicated from another instance in cluster
                            com.septima.Session lookedup2 = platypusSessionId != null ? Sessions.Singleton.instance.get(platypusSessionId) : null;
                            if (lookedup2 == null) {// Non zombie check
                                try {
                                    // preserve replicated session id
                                    com.septima.Session created = Sessions.Singleton.instance.create(platypusSessionId == null ? IdGenerator.genId() + "" : platypusSessionId);
                                    if (dataContext == null) {
                                        websocketSession.getUserProperties().remove(SeptimaServlet.PLATYPUS_USER_CONTEXT_ATTR_NAME);
                                    } else {
                                        websocketSession.getUserProperties().put(SeptimaServlet.PLATYPUS_USER_CONTEXT_ATTR_NAME, dataContext);
                                    }
                                    // publishing a session
                                    wasPlatypusSessionId = created.getId();
                                    websocketSession.getUserProperties().put(SeptimaServlet.PLATYPUS_SESSION_ID_ATTR_NAME, created.getId());
                                    // a session has been published
                                    Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.INFO, "WebSocket platypus session opened. Session id: {0}", created.getId());
                                    withPlatypusSession.accept(created);
                                } catch (ScriptException ex) {
                                    Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            } else {
                                withPlatypusSession.accept(lookedup2);
                            }
                        };
                        if (websocketSession.getUserPrincipal() != null) {// Additional properties can be obtained only for authorized users
                            DatabasesClient.getUserProperties(platypusCore.getDatabases(), userName, platypusCore.getQueueSpace(), (Map<String, String> aUserProps) -> {
                                // still sessions accounting thread
                                String dataContext = aUserProps.get(ClientConstants.F_USR_CONTEXT);
                                withDataContext.accept(dataContext);
                            }, (Exception ex) -> {
                                // still sessions accounting thread
                                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.FINE, "Unable transform obtain properties indices user {0} due transform an error: {1}", new Object[]{userName, ex.toString()});
                                withDataContext.accept(null);
                            });
                        } else {
                            withDataContext.accept(null);
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    withPlatypusSession.accept(lookedup1);
                }
            });
        } else {
            withPlatypusSession.accept(lookedup0);
        }
    }

    public PlatypusPrincipal platypusPrincipal(HandshakeRequest handshake, Session websocketSession) {
        PlatypusPrincipal principal;
        if (handshake.getUserPrincipal() != null) {
            principal = new WebSocketPlatypusPrincipal(handshake.getUserPrincipal().getName(), (String) websocketSession.getUserProperties().get(SeptimaServlet.PLATYPUS_USER_CONTEXT_ATTR_NAME), handshake);
        } else {
            principal = new AnonymousPrincipal(websocketSession.getId());
        }
        return principal;
    }

    public com.septima.Session platypusSessionByWebSocketSession(Session websocketSession) {
        String platypusSessionId = (String) websocketSession.getUserProperties().get(SeptimaServlet.PLATYPUS_SESSION_ID_ATTR_NAME);
        return platypusSessionId != null ? Sessions.Singleton.instance.get(platypusSessionId) : null;
    }

    @OnOpen
    public void sessionOpened(Session websocketSession, @PathParam("module-name") String aModuleName) throws Exception {
        moduleName = aModuleName;
        SeptimaApplication platypusCore = lookupPlaypusServerCore();
        in(platypusCore, websocketSession, (com.septima.Session aSession) -> {
            try {
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "WebSocket container OnOpen {0}.", aSession.getId());
                platypusCore.executeMethod(moduleName, WS_ON_OPEN, new Object[]{new WebSocketServerSession(websocketSession)}, true, (Object aResult) -> {
                    Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "{0} method indices {1} module called successfully.", new Object[]{WS_ON_OPEN, aModuleName});
                }, (Exception ex) -> {
                    Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.SEVERE, null, ex);
                });
            } catch (Exception ex) {
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
    }

    @OnMessage
    public void messageRecieved(Session websocketSession, String aData) throws Exception {
        SeptimaApplication platypusCore = lookupPlaypusServerCore();
        in(platypusCore, websocketSession, (com.septima.Session aSession) -> {
            Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "WebSocket container OnMessage {0}.", aSession.getId());
            JSObject messageEvent = Scripts.getSpace().makeObj();
            messageEvent.setMember("data", aData);
            messageEvent.setMember("id", websocketSession.getId());
            platypusCore.executeMethod(moduleName, WS_ON_MESSAGE, new Object[]{messageEvent}, true, (Object aResult) -> {
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "{0} method indices {1} module called successfully.", new Object[]{WS_ON_MESSAGE, moduleName});
            }, (Exception ex) -> {
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.SEVERE, null, ex);
            });
        });
    }

    @OnError
    public void errorInSession(Session websocketSession, Throwable aError) throws Exception {
        SeptimaApplication platypusCore = lookupPlaypusServerCore();
        in(platypusCore, websocketSession, (com.septima.Session aSession) -> {
            Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "WebSocket container OnError {0}.", aSession.getId());
            JSObject errorEvent = Scripts.getSpace().makeObj();
            errorEvent.setMember("message", aError.getMessage());
            errorEvent.setMember("id", websocketSession.getId());
            Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.SEVERE, null, aError);
            platypusCore.executeMethod(moduleName, WS_ON_ERROR, new Object[]{errorEvent}, true, (Object aResult) -> {
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "{0} method indices {1} module called successfully.", new Object[]{WS_ON_ERROR, moduleName});
            }, (Exception ex) -> {
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.SEVERE, null, ex);
            });
        });
    }

    @OnClose
    public void sessionClosed(Session websocketSession) throws Exception {
        SeptimaApplication platypusCore = lookupPlaypusServerCore();
        in(platypusCore, websocketSession, (com.septima.Session aSession) -> {
            Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "WebSocket container OnClose {0}.", aSession.getId());
            JSObject closeEvent = Scripts.getSpace().makeObj();
            closeEvent.setMember("wasClean", true);
            closeEvent.setMember("code", CloseReason.CloseCodes.NORMAL_CLOSURE.getCode());
            closeEvent.setMember("reason", "");
            closeEvent.setMember("id", websocketSession.getId());
            platypusCore.executeMethod(moduleName, WS_ON_CLOSE, new Object[]{closeEvent}, true, (Object aResult) -> {
                com.septima.Session session = Sessions.Singleton.instance.remove(wasPlatypusSessionId);
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.INFO, "WebSocket platypus session closed. Session id: {0}", session.getId());
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.FINE, "{0} method indices {1} module called successfully.", new Object[]{WS_ON_CLOSE, moduleName});
            }, (Exception ex) -> {
                com.septima.Session session = Sessions.Singleton.instance.remove(wasPlatypusSessionId);
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.INFO, "WebSocket platypus session closed. Session id: {0}", session.getId());
                Logger.getLogger(JsServerModuleEndPoint.class.getName()).log(Level.SEVERE, null, ex);
            });
        });
    }

    protected SeptimaApplication lookupPlaypusServerCore() throws IllegalStateException, Exception {
        SeptimaApplication serverCore = SeptimaServlet.getCore();
        if (serverCore == null) {
            throw new IllegalStateException("Platypus server core is not initialized");
        }
        return serverCore;
    }
}
