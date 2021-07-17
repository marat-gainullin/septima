package com.septima.application;

import com.septima.application.endpoint.SqlEntitiesDataEndPoint;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SqlEntitiesDataEndPointTest extends SqlEntitiesEndPointTest {

    private static final AtomicInteger sequence = new AtomicInteger();

    @BeforeClass
    public static void setup() throws SQLException, NamingException {
        initData();
        initServlets();
    }

    @AfterClass
    public static void tearDown() throws SQLException, NamingException {
        ServletContextEvent scEvent = Mockito.mock(ServletContextEvent.class);
        appInit.contextDestroyed(scEvent);
        appInit = null;
        config = null;
        TestDataSource.unbind();
        h2.close();
        h2 = null;
    }

    @Test
    public void getPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Public access to 'pets' is not allowed\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void getPetsReadRoles() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public-read-roles", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Read access to collection 'pets-public-read-roles' data requires one of the following roles: ['boss']\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void getPetsPublic() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-snapshot-public", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        System.out.println("getPetsPublic: \n" + requestResult.getBody());
        assertEquals("[" +
                "{\"birthdate\":null,\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Druzhok\",\"pets_id\":1.42841880961396E14}," +
                "{\"birthdate\":\"2015-04-28T21:00:00.000+0000\",\"owner_id\":1.42841834950629E14,\"type_id\":1.42841300155478E14,\"name\":\"Vasya\",\"pets_id\":1.42841883974964E14}," +
                "{\"birthdate\":null,\"owner_id\":1.42841788496711E14,\"type_id\":1.4285004671685E14,\"name\":\"Pik\",\"pets_id\":1.43059430815594E14}" +
                "]", requestResult.getBody());
    }

    @Test
    public void getPetsPublicWithParameters() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public-parameters", SqlEntitiesDataEndPoint::new, Map.of(
                "flag", new String[]{"true"},
                "owner_id", new String[]{"1.42841834950629E14"},
                "birthFrom", new String[]{"2015-04-28T00:00:00.000+0000"},
                "name", new String[]{"Vasya"}
        ));
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        assertEquals("[" +
                "{\"birthdate\":\"2015-04-28T21:00:00.000+0000\",\"owner_id\":1.42841834950629E14,\"type_id\":1.42841300155478E14,\"name\":\"Vasya\",\"pets_id\":1.42841883974964E14}" +
                "]", requestResult.getBody());
    }

    @Test
    public void getPetDruzhok() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public/142841880961396", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        assertEquals(
                "{\"birthdate\":null,\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Druzhok\",\"pets_id\":1.42841880961396E14}",
                requestResult.getBody()
        );
    }

    @Test
    public void getAbsentCollection() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/absent-collection/87686", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'absent-collection/87686 or absent-collection' is not found.\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void getAbsentRootCollection() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/absent-collection", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'absent-collection' is not found.\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void getPetAbsent() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public/87686", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'pets-public' doesn't contain an instance with a key: pets_id = 87686.0\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void getPetAbsentBadKeyFormat() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockOut("/pets-public/tttttt", SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, requestResult.getStatus());
        assertNull(requestResult.getBody());
    }

    @Test
    public void postInAbsentCollection() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public/87686", "", METHOD_POST, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'pets-public/87686' is not found.\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void postToPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets", "", METHOD_POST, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Public access to 'pets' is not allowed\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void postToPetsWriteRoles() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public-write-roles", "", METHOD_POST, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Write access to collection 'pets-public-write-roles' data requires one of the following roles: ['boss']\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void postToPublicPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        int newKey = sequence.incrementAndGet();
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public",
                "{\"birthdate\":\"2015-04-30T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Kin\",\"pets_id\":" + newKey + "}",
                METHOD_POST, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_CREATED, requestResult.getStatus());
        assertEquals("{\"key\":\"" + newKey + "\"}", requestResult.getBody());
        assertEquals("http://localhost/septima-test-application/public/pets-public/" + newKey, requestResult.getLocation());
    }

    @Test
    public void postToPublicPetsBadKeyFormat() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public",
                "{\"birthdate\":\"2015-04-30T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Kin\",\"pets_id\":tttt}",
                METHOD_POST, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, requestResult.getStatus());
        assertNull(requestResult.getLocation());
        assertNull(requestResult.getBody());
    }

    @Test
    public void postToPublicPetsBlankKey() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public",
                "{\"birthdate\":\"2015-04-40T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Gosha\"}",
                METHOD_POST, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_CREATED, requestResult.getStatus());
        String bodyUpToKey = "{\"key\":\"";
        assertTrue(requestResult.getBody().startsWith(bodyUpToKey));
        assertTrue(requestResult.getBody().length() > bodyUpToKey.length());
        String headerUpToKey = "http://localhost/septima-test-application/public/pets-public/";
        assertTrue(requestResult.getLocation().startsWith(headerUpToKey));
        assertTrue(requestResult.getLocation().length() > headerUpToKey.length());
    }

    @Test
    public void putInAbsentCollection() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/absent/87686", "", METHOD_PUT, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'absent' is not found.\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void putInPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets/8899", "", METHOD_PUT, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Public access to 'pets' is not allowed\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void putInPetsWriteRoles() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public-write-roles/89908", "", METHOD_PUT, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Write access to collection 'pets-public-write-roles' data requires one of the following roles: ['boss']\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void putInPublicPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public/143059430815590",
                "{\"birthdate\":\"2015-04-30T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Kin\",\"pets_id\":21345}",
                METHOD_PUT, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        assertNull(requestResult.getBody());
    }

    @Test
    public void putInPublicPetsAbsentPet() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public/542842880961396",
                "{\"birthdate\":\"2015-04-30T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Kin\",\"pets_id\":21345}",
                METHOD_PUT, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'pets-public' doesn't contain an instance with a key: pets_id = 542842880961396\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void putInPublicPetsInvalidKey() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public/tttt",
                "{\"birthdate\":\"2015-04-30T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Kin\"}",
                METHOD_PUT, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'pets-public' doesn't contain an instance with a key: pets_id = tttt\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void putWholePublicPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public",
                "{\"birthdate\":\"2015-04-40T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Gosha\"}",
                METHOD_PUT, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Can't update whole collection: 'pets-public'. Update of a whole collection is not supported\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void deleteFromAbsentCollection() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/absent/87686", "", METHOD_DELETE, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'absent' is not found.\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void deleteFromPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets/8899", "", METHOD_DELETE, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Public access to 'pets' is not allowed\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void deleteFromPetsWriteRoles() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut("/pets-public-write-roles/89908", "", METHOD_DELETE, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_FORBIDDEN, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Write access to collection 'pets-public-write-roles' data requires one of the following roles: ['boss']\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void deleteFromPublicPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public/143059430815594",
                "",
                METHOD_DELETE, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_OK, requestResult.getStatus());
        assertNull(requestResult.getBody());
    }

    @Test
    public void deleteFromPublicPetsAbsentPet() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public/542842880961396",
                "",
                METHOD_DELETE, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'pets-public' doesn't contain an instance with a key: pets_id = 542842880961396\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void deleteFromPublicPetsInvalidKey() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public/tttt",
                "{\"birthdate\":\"2015-04-30T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Kin\"}",
                METHOD_DELETE, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_NOT_FOUND, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Collection 'pets-public' doesn't contain an instance with a key: pets_id = tttt\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

    @Test
    public void deleteWholePublicPets() throws ServletException, IOException, InterruptedException, ExecutionException {
        CompletableFuture<RequestResult> response = mockInOut(
                "/pets-public",
                "{\"birthdate\":\"2015-04-40T21:00:00.000+0000\",\"owner_id\":1.42841788496711E14,\"type_id\":1.42841300122653E14,\"name\":\"Gosha\"}",
                METHOD_DELETE, SqlEntitiesDataEndPoint::new);
        RequestResult requestResult = response.get();
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, requestResult.getStatus());
        assertTrue(requestResult.getBody().contains("\"description\":\"Can't delete whole collection: 'pets-public'. Delete of a whole collection is not supported\""));
        assertTrue(requestResult.getBody().contains("\"status\":" + requestResult.getStatus()));
    }

}
