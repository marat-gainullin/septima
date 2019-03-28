package com.septima.application;

import com.septima.application.endpoint.SqlEntitiesCommitEndPoint;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class SqlEntitiesCommitEndPointTest extends SqlEntitiesEndPointTest {

    @BeforeClass
    public static void setup() throws SQLException, NamingException {
        initData();
        initServlets();
    }

    @AfterClass
    public static void tearDown() throws SQLException, NamingException {
        ServletContextEvent scEvent = Mockito.mock(ServletContextEvent.class);
        dataInit.contextDestroyed(scEvent);
        dataInit = null;
        config = null;
        TestDataSource.unbind();
        h2.close();
        h2 = null;
    }

    static String loadResource(String resourceName) throws IOException {
        try (BufferedReader buffered = new BufferedReader(new InputStreamReader(SqlEntitiesCommitEndPointTest.class.getResourceAsStream(resourceName), StandardCharsets.UTF_8))) {
            return buffered.lines().collect(Collectors.joining("\n"));
        }
    }

    @Test
    public void get() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public", SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Not implemented\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));

        assertNull(requestResult.getLocation());
    }

    @Test
    public void post() throws ServletException, IOException, InterruptedException, ExecutionException {
        String commitLog = loadResource("/commit.log.json");
        CompletableFuture<RequestResult> response = mockInOut("", commitLog, METHOD_POST, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        assertEquals("{\"affected\":5}", requestResult.getBody());
        assertNull(requestResult.getLocation());
    }

    @Test
    public void postEntityMissing() throws ServletException, IOException, InterruptedException, ExecutionException {
        String commitLog = loadResource("/entity-missing-commit.log.json");
        CompletableFuture<RequestResult> response = mockInOut("", commitLog, METHOD_POST, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Commit log entry have to contain 'entity' property\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }

    @Test
    public void postKindMissing() throws ServletException, IOException, InterruptedException, ExecutionException {
        String commitLog = loadResource("/kind-missing-commit.log.json");
        CompletableFuture<RequestResult> response = mockInOut("", commitLog, METHOD_POST, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Commit log entry have to contain 'kind' property\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }

    @Test
    public void postKindUnknown() throws ServletException, IOException, InterruptedException, ExecutionException {
        String commitLog = loadResource("/kind-unknown-commit.log.json");
        CompletableFuture<RequestResult> response = mockInOut("", commitLog, METHOD_POST, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Unknown commit log entry kind: 'explode'\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }

    @Test
    public void postAbsentEntity() throws ServletException, IOException, InterruptedException, ExecutionException {
        String commitLog = loadResource("/absent-entity-commit.log.json");
        CompletableFuture<RequestResult> response = mockInOut("", commitLog, METHOD_POST, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'absent-entity' is not found.\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }

    @Test
    public void postNotPublicEntity() throws ServletException, IOException, InterruptedException, ExecutionException {
        String commitLog = loadResource("/not-public-entity-commit.log.json");
        CompletableFuture<RequestResult> response = mockInOut("", commitLog, METHOD_POST, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Public access to 'pets' is not allowed\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }

    @Test
    public void postPublicWriteRolesEntity() throws ServletException, IOException, InterruptedException, ExecutionException {
        String commitLog = loadResource("/public-write-roles-entity-commit.log.json");
        CompletableFuture<RequestResult> response = mockInOut("", commitLog, METHOD_POST, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Write access to collection 'pets-public-write-roles' data requires one of the following roles: ['boss']\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }

    @Test
    public void put() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public", "", METHOD_PUT, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Not implemented\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }

    @Test
    public void delete() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public", "", METHOD_DELETE, SqlEntitiesCommitEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Not implemented\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
        assertNull(requestResult.getLocation());
    }
}
