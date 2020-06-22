package com.septima.application;

import com.septima.application.endpoint.SqlEntitiesEndPoint;
import org.h2.tools.RunScript;
import org.mockito.Mockito;

import javax.naming.NamingException;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;

public class SqlEntitiesEndPointTest {

    static final String METHOD_POST = "POST";
    static final String METHOD_PUT = "PUT";
    static final String METHOD_DELETE = "DELETE";
    static final String SEPTIMA_CONTEXT_NAME = "septima-test-application";
    static final String SERVLET_PATH = "/public";
    static volatile ServletContextListener appInit;
    static volatile ServletConfig config;
    static volatile Connection h2;

    static void initData() throws SQLException, NamingException {
        h2 = DriverManager.getConnection(TestDataSource.DATA_SOURCE_URL, TestDataSource.DATA_SOURCE_USER, TestDataSource.DATA_SOURCE_PASSWORD);
        RunScript.main(
                "-url", TestDataSource.DATA_SOURCE_URL,
                "-user", TestDataSource.DATA_SOURCE_USER,
                "-password", TestDataSource.DATA_SOURCE_PASSWORD,
                "-script", "base.sql"
        );
        System.out.println("Test database filled.");
        TestDataSource.bind();
        System.out.println("Test data source bound.");
        System.setProperty("com.septima.entities.compile", "true");
    }

    static void initServlets() {
        ServletContext servletContext = Mockito.mock(ServletContext.class);
        Mockito.when(servletContext.getServletContextName()).thenReturn(SEPTIMA_CONTEXT_NAME);
        Mockito.when(servletContext.getInitParameterNames()).thenReturn(Collections.enumeration(Set.of(
                "data.source",
                "futures.executor",
                "jdbc.max.threads",
                "mail.max.threads",
                "scope.queue.size",
                "resources.entities.path"
        )));
        Mockito.when(servletContext.getInitParameter("data.source")).thenReturn(TestDataSource.DATA_SOURCE_NAME);
        Mockito.when(servletContext.getInitParameter("futures.executor")).thenReturn("dummy-futures-executor");
        Mockito.when(servletContext.getInitParameter("jdbc.max.threads")).thenReturn("10");
        Mockito.when(servletContext.getInitParameter("mail.max.threads")).thenReturn("12");
        Mockito.when(servletContext.getInitParameter("scope.queue.size")).thenReturn("512");
        Mockito.when(servletContext.getInitParameter("resources.entities.path")).thenReturn("entities");
        config = Mockito.mock(ServletConfig.class);
        Mockito.when(config.getServletContext()).thenReturn(servletContext);

        appInit = new ApplicationInit.OnContext();
        ServletContextEvent scEvent = Mockito.mock(ServletContextEvent.class);
        Mockito.when(scEvent.getServletContext()).thenReturn(servletContext);
        appInit.contextInitialized(scEvent);
    }

    static CompletableFuture<RequestResult> mockOut(String pathInfo, Supplier<SqlEntitiesEndPoint> endPointSource) throws ServletException, IOException {
        return mockOut(pathInfo, endPointSource, Map.of());
    }

    static CompletableFuture<RequestResult> mockOut(String pathInfo, Supplier<SqlEntitiesEndPoint> endPointSource, Map<String, String[]> parameters) throws ServletException, IOException {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        CompletableFuture<RequestResult> responsing = new CompletableFuture<>();
        RequestResult requestResult = new RequestResult();

        AsyncContext asyncContext = Mockito.mock(AsyncContext.class);
        Mockito.when(asyncContext.getRequest()).thenReturn(request);
        Mockito.when(asyncContext.getResponse()).thenReturn(response);
        Mockito.when(asyncContext.getTimeout()).thenReturn(Long.MAX_VALUE);
        Mockito.doAnswer(invocation -> {
            if (requestResult.status == 0) { // Like a container does
                requestResult.status = HttpServletResponse.SC_OK;
            }
            responsing.complete(requestResult);
            return null;
        }).when(asyncContext).complete();

        Mockito.when(request.getMethod()).thenReturn("GET");
        Mockito.when(request.startAsync()).thenReturn(asyncContext);
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost/" + SEPTIMA_CONTEXT_NAME + SERVLET_PATH + pathInfo));
        Mockito.when(request.getServletPath()).thenReturn(SERVLET_PATH);
        Mockito.when(request.getPathInfo()).thenReturn(pathInfo);
        Mockito.when(request.getParameterMap()).thenReturn(parameters);

        ServletOutputStream out = Mockito.mock(ServletOutputStream.class);
        Mockito.doAnswer(invocation -> {
            byte[] data = invocation.getArgument(0);
            requestResult.body = new String(data, StandardCharsets.UTF_8);
            return null;
        }).when(out).write(any(byte[].class));
        Mockito.when(out.isReady())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        Mockito.doAnswer(invocation -> {
            WriteListener wl = invocation.getArgument(0);
            wl.onWritePossible();
            return null;
        }).when(out).setWriteListener(any(WriteListener.class));

        Mockito.when(response.getOutputStream()).thenReturn(out);
        Mockito.doAnswer(invocation -> {
            requestResult.status = invocation.getArgument(0);
            return null;
        }).when(response).setStatus(any(Integer.class));
        Mockito.when(response.getStatus()).then(invocation -> requestResult.status);

        SqlEntitiesEndPoint endpoint = endPointSource.get();
        endpoint.init(config);

        endpoint.service(request, response);

        return responsing;
    }

    static CompletableFuture<RequestResult> mockInOut(String pathInfo, String inBody, String method, Supplier<SqlEntitiesEndPoint> endPointSource) throws ServletException, IOException {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        CompletableFuture<RequestResult> responsing = new CompletableFuture<>();
        RequestResult requestResult = new RequestResult();

        AsyncContext asyncContext = Mockito.mock(AsyncContext.class);
        Mockito.when(asyncContext.getRequest()).thenReturn(request);
        Mockito.when(asyncContext.getResponse()).thenReturn(response);
        Mockito.when(asyncContext.getTimeout()).thenReturn(Long.MAX_VALUE);
        Mockito.doAnswer(invocation -> {
            Runnable action = invocation.getArgument(0);
            ForkJoinPool.commonPool().execute(action);
            return null;
        }).when(asyncContext).start(any(Runnable.class));
        Mockito.doAnswer(invocation -> {
            if (requestResult.status == 0) { // Like a container does
                requestResult.status = HttpServletResponse.SC_OK;
            }
            responsing.complete(requestResult);
            return null;
        }).when(asyncContext).complete();

        Mockito.when(request.getMethod()).thenReturn(method);
        Mockito.when(request.startAsync()).thenReturn(asyncContext);
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost/" + SEPTIMA_CONTEXT_NAME + SERVLET_PATH + pathInfo));
        Mockito.when(request.getServletPath()).thenReturn(SERVLET_PATH);
        Mockito.when(request.getPathInfo()).thenReturn(pathInfo);
        Mockito.when(request.getContentType()).thenReturn("application/json;charset=utf-8");

        ServletOutputStream out = Mockito.mock(ServletOutputStream.class);
        Mockito.doAnswer(invocation -> {
            byte[] data = invocation.getArgument(0);
            requestResult.body = new String(data, StandardCharsets.UTF_8);
            return null;
        }).when(out).write(any(byte[].class));
        Mockito.when(out.isReady())
                .thenReturn(true, true, true, true, false);
        Mockito.doAnswer(invocation -> {
            WriteListener wl = invocation.getArgument(0);
            wl.onWritePossible();
            return null;
        }).when(out).setWriteListener(any(WriteListener.class));

        Mockito.when(response.getOutputStream()).thenReturn(out);
        Mockito.doAnswer(invocation -> {
            requestResult.status = invocation.getArgument(0);
            return null;
        }).when(response).setStatus(any(Integer.class));
        Mockito.doAnswer(invocation -> {
            String headerName = invocation.getArgument(0);
            String headerValue = invocation.getArgument(1);
            if ("location".equalsIgnoreCase(headerName)) {
                requestResult.location = headerValue;
            }
            return null;
        }).when(response).setHeader(any(String.class), any(String.class));
        Mockito.when(response.getStatus()).then(invocation -> requestResult.status);

        ServletInputStream in = Mockito.mock(ServletInputStream.class);
        AtomicReference<ReadListener> inReadListener = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            inReadListener.set(invocation.getArgument(0));
            inReadListener.get().onDataAvailable();
            return null;
        }).when(in).setReadListener(any(ReadListener.class));
        Mockito.when(in.isReady()).thenReturn(true);
        ByteArrayInputStream inBodyData = inBody != null ? new ByteArrayInputStream(inBody.getBytes(StandardCharsets.UTF_8)) : null;
        Mockito.when(in.read(any(byte[].class))).then(invocation -> {
            byte[] destination = invocation.getArgument(0);
            int read = inBodyData.read(destination, 0, 1);
            if (read == -1) {
                inReadListener.get().onAllDataRead();
            }
            return read;
        });
        Mockito.when(request.getInputStream()).thenReturn(in);
        Mockito.when(request.getContentLength()).thenReturn(inBodyData != null ? inBodyData.available() : 0);

        SqlEntitiesEndPoint endpoint = endPointSource.get();
        endpoint.init(config);

        endpoint.service(request, response);

        return responsing;
    }

    protected static class RequestResult {
        private int status;
        private String body;
        private String location;

        public int getStatus() {
            return status;
        }

        public String getBody() {
            return body;
        }

        public String getLocation() {
            return location;
        }

    }
}
