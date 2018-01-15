package com.septima;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.septima.application.Data;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.metadata.Parameter;
import com.septima.queries.SqlQuery;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SqlEntitiesPublisher extends HttpServlet {

    private static final String JSON_CONTENT_TYPE = "application/json;charset=utf-8";

    private static Object parseParameterValue(String aValue, GenericType aType) {
        if (aValue != null) {
            switch (aType) {
                case DATE:
                    StdDateFormat format = new StdDateFormat();
                    try {
                        return format.parse(aValue);
                    } catch (ParseException ex) {
                        throw new IllegalStateException(ex);
                    }
                case BOOLEAN:
                    return Boolean.parseBoolean(aValue);
                case DOUBLE:
                    return Double.parseDouble(aValue);
                case LONG:
                    return Long.parseLong(aValue);
                default:
                    return aValue;
            }
        } else {
            return aValue;
        }
    }


    private static final ObjectWriter JSON = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .writer();

    private volatile SqlEntities entities;

    @Override
    public void init() {
        entities = Data.getInstance().getEntities();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String path = req.getPathInfo();
        if (path != null && path.length() > 1) {
            SqlEntity entity = entities.loadEntity(path.substring(1));
            SqlQuery query = entity.toQuery();
            Map<String, String> httpParameters = req.getParameterMap().entrySet().stream()
                    .filter(e -> e.getValue() != null && e.getValue().length == 1)
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()[0]));
            Map<String, Object> parameters = entity.getParameters().values().stream()
                    .collect(Collectors.toMap(Parameter::getName, p -> parseParameterValue(httpParameters.get(p.getName()), p.getType())));
            AsyncContext ctx = req.startAsync();
            query.requestData(parameters)
                    .thenAccept(data -> {
                        try {
                            resp.setContentType(JSON_CONTENT_TYPE);
                            try (OutputStream out = resp.getOutputStream()) {
                                JSON.writeValue(out, data);
                                out.flush();
                            }
                            ctx.complete();
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    })
                    .exceptionally(e -> {
                        try {
                            Logger.getLogger(SqlEntitiesPublisher.class.getName()).log(Level.SEVERE, null, e);
                            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                            ctx.complete();
                            return null;
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
        } else {
            super.doGet(req, resp);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doGet(req, resp);
    }
}
