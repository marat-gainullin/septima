package com.septima.http;

import com.septima.Indexer;
import com.septima.Requests;
import com.septima.ResidentModules;
import com.septima.SeptimaApplication;
import com.septima.Config;
import com.septima.indexer.ScriptDocument;
import com.septima.jdbc.DataSources;
import com.septima.script.JsObjectException;
import com.septima.script.Scripts;
import com.septima.util.IdGenerator;
import com.septima.util.JsonUtils;
import org.omg.CORBA.Request;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.*;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Platypus HTTP servlet implementation
 *
 * @author mg
 */
public class SeptimaServlet extends HttpServlet {

    public static final String CORE_MISSING_MSG = "Application core havn't been initialized";
    public static final String SESSION_MISSING_MSG = "Session %s missing";
    public static final String HTTP_SESSION_MISSING_MSG = "Container's session missing";
    public static final String PLATYPUS_SESSION_MISSING_MSG = "Platypus session missing";
    public static final String ERRORRESPONSE_ERROR_MSG = "Error while sending ErrorResponse";
    public static final String UNKNOWN_REQUEST_MSG = "Unknown http request has arrived. It's type is %d";
    public static final String REQUEST_PROCESSSING_ERROR_MSG = "Request processsing error";
    public static final String SUBJECT_CONTEXT_KEY = "javax.security.auth.Subject.container";
    public static final String HTTP_HOST_OBJECT_NAME = "http";
    public static final String EXCEL_CONTENT_TYPE = "application/xls";
    public static final String EXCELX_CONTENT_TYPE = "application/xlsx";
    public static final String HTML_CONTENT_TYPE = "text/html";
    public static final String TEXT_CONTENT_TYPE = "text/plain";
    public static final String PLATYPUS_SESSION_ID_ATTR_NAME = "platypus-session-id";
    public static final String PLATYPUS_USER_CONTEXT_ATTR_NAME = "platypus-user-context";

    private static volatile SeptimaApplication application;
    private String realRootPathName;
    private Config platypusConfig;
    private com.septima.http.RestPoints restPoints;
    private ExecutorService containerExecutor;
    private ExecutorService selfExecutor;

    @Override
    public void init(ServletConfig config) throws ServletException {
        try {
            super.init(config);
            realRootPathName = config.getServletContext().getRealPath("/");
            platypusConfig = Config.parse(config);
            try {
                containerExecutor = (ExecutorService) InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
            } catch (NamingException ex) {
                try {
                    containerExecutor = (ExecutorService) InitialContext.doLookup("java:comp/env/concurrent/ThreadPool");
                } catch (NamingException ex1) {
                    selfExecutor = new ThreadPoolExecutor(platypusConfig.getMaximumLpcThreads(), platypusConfig.getMaximumLpcThreads(),
                            1L, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>(platypusConfig.getMaximumLpcQueueSize()),
                            new PlatypusThreadFactory("platypus-worker-", false));
                    ((ThreadPoolExecutor) selfExecutor).allowCoreThreadTimeOut(true);
                }
            }
            File realRoot = new File(realRootPathName);
            if (realRoot.exists() && realRoot.isDirectory()) {
                final ResidentModules residentModules = new ResidentModules();
                final com.septima.ModulesSecurity modulesSecurity = new com.septima.ModulesSecurity();
                restPoints = new com.septima.http.RestPoints();
                Path projectRoot = Paths.get(realRoot.toURI());
                Path appFolder = platypusConfig.getSourcePath() != null ? projectRoot.resolve(platypusConfig.getSourcePath()) : projectRoot;
                Path apiFolder = projectRoot.resolve("WEB-INF" + File.separator + "classes");
                Indexer indexer = new Indexer(appFolder, apiFolder, (String aModuleName, ScriptDocument.ModuleDocument aModuleDocument, File aFile) -> {
                    residentModules.scanned(aModuleName, aModuleDocument, aFile);
                    restPoints.scanned(aModuleName, aModuleDocument, aFile);
                    modulesSecurity.scanned(aModuleName, aModuleDocument, aFile);
                });
                DataSources basesProxy = new DataSources(platypusConfig.getDefaultDataSourceName(), indexer, true, residentModules.getValidators(), platypusConfig.getMaximumJdbcThreads());
                QueriesProxy<SqlQuery> queries = new LocalQueriesProxy(basesProxy, indexer);
                basesProxy.setQueries(queries);
                application = new SeptimaApplication(indexer, new LocalModulesProxy(indexer, new ModelsDocuments(), platypusConfig.getAppElementName()), queries, basesProxy, modulesSecurity, platypusConfig.getAppElementName(), com.septima.Sessions.Singleton.instance, platypusConfig.getMaximumSpaces());
                Scripts.initBIO(platypusConfig.getMaximumMailTreads());
                ScriptedResource.init(application, apiFolder, platypusConfig.isGlobalAPI());
                Scripts.initTasks(containerExecutor != null ? containerExecutor /* J2EE 7+ */ : selfExecutor /* Other environment */);
                if (platypusConfig.isWatch()) {
                    // TODO: Uncomment after watcher refactoring
                    //indexer.watch();
                }
            } else {
                throw new IllegalArgumentException("ApplicationEndPoint path: " + realRootPathName + " doesn't point transform existent directory.");
            }
        } catch (Throwable ex) {
            throw new ServletException(ex);
        }
    }

    public static SeptimaApplication getCore() {
        return application;
    }

    @Override
    public void destroy() {
        if (platypusConfig.isWatch()) {
            try {
                application.getIndexer().unwatch();
            } catch (Exception ex) {
                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        Scripts.shutdown();
        if (application.getDatabases() != null) {
            try {
                application.getDatabases().shutdown();
            } catch (InterruptedException ex) {
                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (selfExecutor != null) {
            selfExecutor.shutdown();
            try {
                selfExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        super.destroy();
    }

    protected static final String PUB_CONTEXT = "/pub/";

    protected boolean checkUpload(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        if (request.getContentType() != null && request.getContentType().contains("multipart/form-data")) {
            List<StringBuilder> uploadedLocations = new ArrayList();
            for (Part part : request.getParts()) {
                String dispositionHeader = part.getHeader("content-disposition");
                if (dispositionHeader != null) {
                    Pattern fileNamePattern = Pattern.compile(".*filename=.*\"(.+)\".*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
                    Matcher m = fileNamePattern.matcher(dispositionHeader);
                    String fileName = null;
                    if (m.matches()) {
                        fileName = m.group(1);
                    }
                    if (fileName != null && !fileName.isEmpty()) {
                        StringBuilder uploadedFileName = new StringBuilder();
                        uploadedFileName.append(IdGenerator.genId()).append("-").append(fileName);
                        try {
                            try {
                                part.write(uploadedFileName.toString());
                            } catch (IOException ex) {
                                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, "Falling back transform copy implementation", ex);
                                String realPath = request.getServletContext().getRealPath(PUB_CONTEXT + uploadedFileName.toString());
                                try (InputStream fin = part.getInputStream(); FileOutputStream fout = new FileOutputStream(realPath)) {
                                    byte[] buffer = new byte[1024 * 16];
                                    int read = fin.read(buffer);
                                    while (read >= 0) {
                                        fout.write(buffer, 0, read);
                                        read = fin.read(buffer);
                                    }
                                }
                            }
                        } finally {
                            part.delete();
                        }
                        StringBuilder uploadedLocation = new StringBuilder();
                        uploadedLocation.append("http://").append(request.getHeader("host")).append(request.getServletContext().getContextPath()).append(PUB_CONTEXT).append(uploadedFileName);
                        uploadedLocations.add(uploadedLocation);
                    }
                }
            }
            com.septima.http.ResponseWriter.writeJsonResponse(JsonUtils.as(uploadedLocations.toArray(new StringBuilder[]{})).toString(), response, null);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (!checkUpload(request, response)) {
            if (application != null) {
                HttpSession httpSession = request.getSession();
                if (httpSession != null) {
                    AsyncContext async = request.startAsync();
                    async.setTimeout(-1);
                    String userName = request.getUserPrincipal() != null ? request.getUserPrincipal().getName() : null;
                    Consumer<Session> withPlatypusSession = (Session aSession) -> {
                        // http executor thread or sessions accounting thread
                        // temporarily session thread 
                        try {
                            handlePlatypusRequest(request, response, httpSession, aSession, async);
                        } catch (Exception ex) {
                            Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex);
                            try {
                                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
                                async.complete();
                            } catch (IOException | IllegalStateException ex1) {
                                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex1);
                            }
                        }
                    };
                    Session lookedup0 = platypusSessionByHttpSession(httpSession);
                    if (lookedup0 == null) {// Zombie check
                        application.getQueueSpace().process(() -> {
                            // sessions accounting thread
                            Session lookedup1 = platypusSessionByHttpSession(httpSession);
                            if (lookedup1 == null) {
                                try {
                                    Consumer<String> withDataContext = (String dataContext) -> {
                                        String platypusSessionId = (String) httpSession.getAttribute(PLATYPUS_SESSION_ID_ATTR_NAME);
                                        // platypusSessionId may be replicated from another instance in cluster
                                        Session lookedup2 = platypusSessionId != null ? com.septima.Sessions.Singleton.instance.get(platypusSessionId) : null;
                                        if (lookedup2 == null) {
                                            try {
                                                // preserve replicated session id
                                                com.septima.Session created = com.septima.Sessions.Singleton.instance.create(platypusSessionId == null ? IdGenerator.genId() + "" : platypusSessionId);
                                                if (dataContext == null) {
                                                    httpSession.removeAttribute(PLATYPUS_USER_CONTEXT_ATTR_NAME);
                                                } else {
                                                    httpSession.setAttribute(PLATYPUS_USER_CONTEXT_ATTR_NAME, dataContext);
                                                }
                                                // publishing a session
                                                httpSession.setAttribute(PLATYPUS_SESSION_ID_ATTR_NAME, created.getId());
                                                // a session has been published
                                                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.INFO, "Http platypus session opened. Session id: {0}", created.getId());
                                                withPlatypusSession.accept(created);
                                            } catch (Exception ex) {
                                                Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex);
                                                try {
                                                    response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
                                                    async.complete();
                                                } catch (IOException | IllegalStateException ex1) {
                                                    Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex1);
                                                }
                                            }
                                        } else {
                                            withPlatypusSession.accept(lookedup2);
                                        }
                                    };
                                    if (request.getUserPrincipal() != null) {// Additional properties can be obtained only for authorized users
                                        DatabasesClient.getUserProperties(application.getDatabases(), userName, application.getQueueSpace(), (Map<String, String> aUserProps) -> {
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
                                    try {
                                        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
                                        async.complete();
                                    } catch (IOException | IllegalStateException ex1) {
                                        Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex1);
                                    }
                                }
                            } else {
                                withPlatypusSession.accept(lookedup1);
                            }
                        });
                    } else {
                        // http executor thread
                        withPlatypusSession.accept(lookedup0);
                    }
                } else {
                    response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, HTTP_SESSION_MISSING_MSG);
                }
            } else {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, CORE_MISSING_MSG);
            }
        }
    }

    public Session platypusSessionByHttpSession(HttpSession httpSession) {
        com.septima.Sessions sessions = application.getSessions();
        String platypusSessionId = (String) httpSession.getAttribute(PLATYPUS_SESSION_ID_ATTR_NAME);
        Session session = platypusSessionId != null ? sessions.get(platypusSessionId) : null;
        return session;
    }

    private static PlatypusPrincipal httpRequestPrincipal(final HttpServletRequest aRequest) {
        HttpSession httpSession = aRequest.getSession(false);
        if (aRequest.getUserPrincipal() != null) {
            return new RequestPrincipal(aRequest.getUserPrincipal().getName(), (String) httpSession.getAttribute(PLATYPUS_USER_CONTEXT_ATTR_NAME), aRequest);
        } else {
            return httpSession != null ? new AnonymousPrincipal("anonymous-" + httpSession.getId()) : new AnonymousPrincipal();
        }
    }

    public SeptimaApplication getServerCore() {
        return application;
    }

    /**
     * Precesses request for PlatypusAPI requests.
     *
     * @param aHttpRequest
     * @param aPlatypusSession
     * @param aHttpResponse
     * @param aHttpSession
     * @throws Exception
     */
    private void handlePlatypusRequest(final HttpServletRequest aHttpRequest, final HttpServletResponse aHttpResponse, HttpSession aHttpSession, Session aPlatypusSession, AsyncContext aAsync) throws Exception {
        // temporarily session thread 
        Request platypusRequest = readPlatypusRequest(aHttpRequest, aHttpResponse);
        if (platypusRequest.getType() == Requests.rqLogout) {
            aHttpRequest.logout();
            aHttpSession.invalidate();
            aHttpResponse.setStatus(HttpServletResponse.SC_OK);
            aAsync.complete();
        } else {
            RequestHandler<Request, Response> handler = (RequestHandler<Request, Response>) RequestHandlerFactory.getHandler(application, platypusRequest);
            if (handler != null) {
                Consumer<Exception> onFailure = (Exception ex) -> {
                    Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, ex.toString());
                    try {
                        if (ex instanceof AccessControlException) {
                            /*
                             // We can't send HttpServletResponse.SC_UNAUTHORIZED without knowlege about login mechanisms
                             // indices J2EE container.
                             AccessControlException accEx = (AccessControlException)ex;
                             aHttpResponse.sendError(accEx.getPermission() instanceof AuthPermission ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN, ex.getMessage());
                             */
                            aHttpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, ex.getMessage());
                        } else if (ex instanceof FileNotFoundException) {
                            aHttpResponse.sendError(HttpServletResponse.SC_NOT_FOUND, ex.getMessage());
                        } else if (ex instanceof JsObjectException) {
                            String errorBody = aPlatypusSession.getSpace().toJson(((JsObjectException) ex).getData());
                            if (aHttpResponse.getStatus() >= 200 && aHttpResponse.getStatus() < 300) {
                                aHttpResponse.setStatus(HttpServletResponse.SC_CONFLICT);
                            }
                            com.septima.http.ResponseWriter.writeJsonResponse(errorBody, aHttpResponse, null);
                        } else if (ex instanceof FlowProviderFailedException) {
                            aHttpResponse.sendError(HttpServletResponse.SC_EXPECTATION_FAILED, "Can't return data with such entity");
                        } else {
                            aHttpResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.getMessage());
                        }
                        aAsync.complete();
                    } catch (IOException ex1) {
                        Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, null, ex1);
                    }
                };
                aPlatypusSession.accessed();
                Scripts.LocalContext context = new Scripts.LocalContext(aHttpRequest, aHttpResponse, httpRequestPrincipal(aHttpRequest), aPlatypusSession);
                aPlatypusSession.getSpace().process(context, () -> {
                    handler.handle(aPlatypusSession, (Response resp) -> {
                        assert Scripts.getSpace() == aPlatypusSession.getSpace();
                        com.septima.http.ResponseWriter writer = new com.septima.http.ResponseWriter(aHttpResponse, aHttpRequest, aPlatypusSession.getSpace(), ((Principal) Scripts.getContext().getPrincipal()).getName(), aAsync);
                        try {
                            resp.accept(writer);
                        } catch (Exception ex) {
                            Logger.getLogger(SeptimaServlet.class.getName()).log(Level.SEVERE, ex.getMessage());
                        }
                    }, (Exception ex) -> {
                        onFailure.accept(ex);
                    });
                });
            } else {
                throw new IllegalStateException("No request handler found");
            }
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left transform edit the code.">
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            processRequest(request, response);
        } catch (Exception ex) {
            throw new ServletException(ex);
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            processRequest(req, resp);
        } catch (Exception ex) {
            throw new ServletException(ex);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        try {
            processRequest(req, resp);
        } catch (Exception ex) {
            throw new ServletException(ex);
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            processRequest(req, resp);
        } catch (Exception ex) {
            throw new ServletException(ex);
        }
    }

    /**
     * Returns a short description indices the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Platypus servlet provides platypus server functionality within a J2EE/Servlet container";
    }// </editor-fold>

    protected Request readPlatypusRequest(HttpServletRequest aHttpRequest, HttpServletResponse aResponse) throws Exception {
        String sType = aHttpRequest.getParameter(PlatypusHttpRequestParams.TYPE);
        if (sType == null && aHttpRequest.getParameter(PlatypusHttpRequestParams.MODULE_NAME) != null && aHttpRequest.getParameter(PlatypusHttpRequestParams.METHOD_NAME) != null) {
            sType = "" + Requests.rqExecuteServerModuleMethod;
        }
        if (sType != null) {
            int rqType = Integer.valueOf(sType);
            Request rq = PlatypusRequestsFactory.create(rqType);
            if (rq != null) {
                com.septima.http.RequestReader reader = new com.septima.http.RequestReader(application, aHttpRequest);
                rq.accept(reader);
                return rq;
            } else {
                throw new Exception(String.format(UNKNOWN_REQUEST_MSG, rqType));
            }
        } else {
            String contextedUri = aHttpRequest.getPathInfo();
            if (contextedUri != null) {
                Map<String, com.septima.http.RpcPoint> methoded = restPoints.getMethoded().get(aHttpRequest.getMethod().toLowerCase());
                if (methoded != null) {
                    String contextedUriHead = contextedUri;
                    while (!contextedUriHead.isEmpty() && !methoded.containsKey(contextedUriHead)) {
                        contextedUriHead = contextedUriHead.substring(0, contextedUriHead.lastIndexOf("/"));
                    }
                    if (methoded.containsKey(contextedUriHead)) {
                        com.septima.http.RpcPoint rpcPoint = methoded.get(contextedUriHead);
                        String tail = contextedUri.substring(contextedUriHead.length());
                        if (tail.startsWith("/")) {
                            tail = tail.substring(1);
                        }
                        if (tail.endsWith("/")) {
                            tail = tail.substring(0, tail.length() - 1);
                        }
                        return new RPCRequest(rpcPoint.getModuleName(), rpcPoint.getMethodName(), new String[]{JsonUtils.s(!tail.isEmpty() ? tail : "").toString()});
                    }
                }
            }
            throw new Exception(String.format("Neither REST endpoint for URI %s, nor API parameters ('%s', '%s', '%s', etc.) found in the request", contextedUri, PlatypusHttpRequestParams.TYPE, PlatypusHttpRequestParams.QUERY_ID, PlatypusHttpRequestParams.MODULE_NAME));
        }
    }
}