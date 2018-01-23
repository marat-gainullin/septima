package com.septima.application;

import com.septima.application.endpoint.SqlEntitiesSchemaEndPoint;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SqlEntitiesSchemaEndPointTest extends SqlEntitiesEndPointTest {

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

    @Test
    public void getPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets", SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertEquals("{\"description\":\"Public access to 'pets' is not allowed\"}", requestResult.getBody());
    }

    @Test
    public void getPetsReadRoles() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public-read-roles", SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertEquals("{\"description\":\"Read access to collection 'pets-public-read-roles' data requires one of the following roles: ['boss']\"}", requestResult.getBody());
    }

    @Test
    public void getPetsPublic() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public", SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        assertEquals("" +
                        "[" +
                        "{\"name\":\"pets_id\",\"type\":\"DOUBLE\",\"nullable\":false,\"description\":\"\",\"pk\":true}," +
                        "{\"name\":\"owner_id\",\"type\":\"DOUBLE\",\"nullable\":false,\"description\":\"\",\"pk\":false}," +
                        "{\"name\":\"type_id\",\"type\":\"DOUBLE\",\"nullable\":false,\"description\":\"\",\"pk\":false}," +
                        "{\"name\":\"name\",\"type\":\"STRING\",\"nullable\":true,\"description\":\"\",\"pk\":false}," +
                        "{\"name\":\"birthdate\",\"type\":\"DATE\",\"nullable\":true,\"description\":\"\",\"pk\":false}" +
                        "]",
                requestResult.getBody());
    }

    @Test
    public void getFieldBirthDate() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public/birthdate", SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        assertEquals(
                "{\"name\":\"birthdate\",\"type\":\"DATE\",\"nullable\":true,\"description\":\"\",\"pk\":false}",
                requestResult.getBody());
    }

    @Test
    public void getAbsentCollection() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/absent-collection/87686", SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertEquals(
                "{\"description\":\"Collection 'absent-collection/87686 or absent-collection' is not found.\"}",
                requestResult.getBody()
        );
    }

    @Test
    public void getAbsentRootCollection() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/absent-collection", SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertEquals(
                "{\"description\":\"Collection 'absent-collection' is not found.\"}",
                requestResult.getBody()
        );
    }

    @Test
    public void getFieldAbsent() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public/87686", SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertEquals(
                "{\"description\":\"Collection 'pets-public' doesn't contain an instance with a key: field.name = 87686\"}",
                requestResult.getBody()
        );
    }

    @Test
    public void post() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public", "", METHOD_POST, SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, requestResult.getStatus());
        assertEquals("{\"description\":\"Not implemented\"}", requestResult.getBody());
        assertNull(requestResult.getLocation());
    }

    @Test
    public void put() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public", "", METHOD_PUT, SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, requestResult.getStatus());
        assertEquals("{\"description\":\"Not implemented\"}", requestResult.getBody());
        assertNull(requestResult.getLocation());
    }

    @Test
    public void delete() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public", "", METHOD_DELETE, SqlEntitiesSchemaEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, requestResult.getStatus());
        assertEquals("{\"description\":\"Not implemented\"}", requestResult.getBody());
        assertNull(requestResult.getLocation());
    }
}
