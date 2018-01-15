package com.septima.application;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ApplicationEndPoint extends HttpServlet {

    private static class Profile {
    }

    private static class Subscription {
    }

    /**
     * Retrieves a list of orders or a single order if its key is specified in an URI.
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Scope.global(Subscription::new, "subscriptions", s -> (String) null);
        Scope.session(Profile::new, "terminal.profile", p -> (String) null, req.getSession());
        /*
                .thenCompose(Scope::bind)
                .thenAccept(c -> {
                    try {
                        resp.getWriter().write(c);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                });
        */
    }

    /**
     * Creates an order in orders collection with the specified {@code Key} in an URI or updates it if an order with this key already present.
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doPut(req, resp);
    }

    /**
     * Creates a new order in orders collection.
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doPost(req, resp);
    }

    /**
     * Deletes an order, specified in an URI.
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doDelete(req, resp);
    }
}
