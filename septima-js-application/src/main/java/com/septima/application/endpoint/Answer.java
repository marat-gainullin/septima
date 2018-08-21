package com.septima.application.endpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.septima.application.exceptions.EndPointException;
import com.septima.application.exceptions.InvalidRequestException;
import com.septima.application.exceptions.NoAccessException;
import com.septima.application.exceptions.NoCollectionException;
import com.septima.application.exceptions.NoImplementationException;
import com.septima.application.exceptions.NoInstanceException;
import com.septima.application.io.RequestBodyReceiver;
import com.septima.application.io.ResponseBodySender;

import javax.servlet.AsyncContext;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Answer {

    private static final String JSON_CONTENT_TYPE = "application/json;charset=utf-8";
    private static final String JSON_CONTENT_TYPE_REQUIRED = "Request is expected to have a json body, i.e. content type should be '" + JSON_CONTENT_TYPE + "'";
    private static final ObjectWriter JSON_WRITER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .writer();
    private static final ObjectReader JSON_VALUE_READER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .reader()
            .forType(Object.class);
    private static final ObjectReader JSON_OBJECT_READER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .reader()
            .forType(Map.class);
    private static final ObjectReader JSON_ARRAY_READER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .reader()
            .forType(List.class);

    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final AsyncContext context;
    private final Executor futuresExecutor;

    public Answer(AsyncContext aContext, Executor aFuturesExecutor) {
        context = aContext;
        request = (HttpServletRequest) aContext.getRequest();
        response = (HttpServletResponse) aContext.getResponse();
        futuresExecutor = aFuturesExecutor;
    }

    public static <K, V> Map<K, V> scalars(Map<K, V[]> aValue) {
        return aValue.entrySet().stream()
                .filter(e -> e.getValue() != null && e.getValue().length == 1)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()[0]));
    }

    private static Throwable unwrapException(Throwable th, Predicate<Throwable> test, int maxDeepness) {
        Throwable unwrapped = th;
        int deepness = 0;
        while (unwrapped.getCause() != null && deepness < maxDeepness && !test.test(unwrapped)) {
            deepness++;
            unwrapped = unwrapped.getCause();
        }
        return unwrapped;
    }

    public HttpServletRequest getRequest() {
        return request;
    }

    public HttpServletResponse getResponse() {
        return response;
    }

    public AsyncContext getContext() {
        return context;
    }

    public void erroneous(String aMessage) {
        erroneous(HttpServletResponse.SC_METHOD_NOT_ALLOWED, aMessage);
    }

    public void erroneous(int aStatus, String aMessage) {
        Logger.getLogger(Answer.class.getName()).log(aStatus >= 500 && aStatus < 600 ? Level.SEVERE : Level.WARNING, aMessage);
        response.setStatus(aStatus);
        context.complete();
    }

    public Void exceptionally(Throwable aTh) {
        Objects.requireNonNull(aTh, "aTh is required argument");
        Throwable th = unwrapException(aTh, t -> t instanceof EndPointException, 16);
        Logger.getLogger(Answer.class.getName()).log(Level.SEVERE, th.getMessage(), th);
        response.setStatus(
                th instanceof NoInstanceException || th instanceof NoCollectionException ? HttpServletResponse.SC_NOT_FOUND :
                        th instanceof NoAccessException ? HttpServletResponse.SC_FORBIDDEN :
                                th instanceof InvalidRequestException || th instanceof NoImplementationException ? HttpServletResponse.SC_METHOD_NOT_ALLOWED :
                                        th instanceof SQLException ? HttpServletResponse.SC_CONFLICT :
                                                HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        if (th instanceof EndPointException && th.getMessage() != null && !th.getMessage().isEmpty()) {
            withJsonObject(Map.of("description", th.getMessage()));
        } else {
            context.complete();
        }
        return null;
    }

    /**
     * It is not recommended to use this method with complex data structures as lists or maps.
     * They will be written, but client code will not get type checking.
     *
     * @param aValue Value to be converted to JSON representation.
     */
    public void withJsonValue(Object aValue) {
        try {
            withContent(JSON_CONTENT_TYPE, JSON_WRITER.writeValueAsBytes(aValue));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public void withJsonObject(Map<String, Object> anInstance) {
        try {
            withContent(JSON_CONTENT_TYPE, JSON_WRITER.writeValueAsBytes(anInstance));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public void withJsonArray(Collection<Map<String, Object>> aData) {
        try {
            withContent(JSON_CONTENT_TYPE, JSON_WRITER.writeValueAsBytes(aData));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public void withContent(String aContentType, byte[] aData) {
        try {
            response.setContentType(aContentType);
            response.getOutputStream().setWriteListener(new ResponseBodySender(aData, context, context::complete));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public void ok() {
        context.complete();
    }

    public void created(String newKey) {
        response.setStatus(HttpServletResponse.SC_CREATED);
        response.setHeader("Location", request.getRequestURL().append("/").append(newKey).toString());
        withJsonObject(Map.of("key", newKey));
    }

    public CompletableFuture<List<Map<String, Object>>> onJsonArray() {
        if (request.getContentType() != null && JSON_CONTENT_TYPE.equalsIgnoreCase(request.getContentType())) {
            return input()
                    .thenApply(data -> {
                        try {
                            return JSON_ARRAY_READER.readValue(data);
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
        } else {
            throw new InvalidRequestException(JSON_CONTENT_TYPE_REQUIRED);
        }
    }

    public CompletableFuture<Map<String, Object>> onJsonObject() {
        if (request.getContentType() != null && JSON_CONTENT_TYPE.equalsIgnoreCase(request.getContentType())) {
            return input()
                    .thenApply(data -> {
                        try {
                            return JSON_OBJECT_READER.readValue(data);
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
        } else {
            throw new InvalidRequestException(JSON_CONTENT_TYPE_REQUIRED);
        }
    }

    public CompletableFuture<Object> onJsonValue() {
        if (request.getContentType() != null && JSON_CONTENT_TYPE.equalsIgnoreCase(request.getContentType())) {
            return input()
                    .thenApply(data -> {
                        try {
                            return JSON_VALUE_READER.readValue(data);
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
        } else {
            throw new InvalidRequestException(JSON_CONTENT_TYPE_REQUIRED);
        }
    }

    public CompletableFuture<byte[]> input() {
        try {
            CompletableFuture<byte[]> receiving = new CompletableFuture<>();
            ServletInputStream in = request.getInputStream();
            in.setReadListener(new RequestBodyReceiver(in, (data) -> {
                receiving.completeAsync(() -> data, futuresExecutor);
            }));
            return receiving;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

}
