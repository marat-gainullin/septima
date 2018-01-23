package com.septima.application;

import com.septima.application.endpoint.Answer;
import com.septima.application.endpoint.HttpEndPoint;
import com.septima.application.exceptions.NoImplementationException;
import com.septima.entities.SqlEntities;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public abstract class AsyncEndPoint extends HttpServlet implements HttpEndPoint {

    protected transient volatile Executor futuresExecutor;
    protected transient volatile SqlEntities entities;

    private void handleAsync(Consumer<Answer> aHandler, AsyncContext aContext) {
        aHandler.accept(new Answer(aContext, futuresExecutor));
    }

    @Override
    public void init() {
        futuresExecutor = Config.lookupExecutor();
        entities = Data.getInstance().getEntities();
    }

    @Override
    public void get(Answer answer) {
        throw new NoImplementationException();
    }

    @Override
    public void post(Answer answer) {
        throw new NoImplementationException();
    }

    @Override
    public void put(Answer answer) {
        throw new NoImplementationException();
    }

    @Override
    public void delete(Answer answer) {
        throw new NoImplementationException();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        handleAsync(answer -> {
            try {
                get(answer);
            } catch (Throwable th) {
                answer.exceptionally(th);
            }
        }, req.startAsync());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
        handleAsync(answer -> {
            try {
                post(answer);
            } catch (Throwable th) {
                answer.exceptionally(th);
            }
        }, req.startAsync());
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) {
        handleAsync(answer -> {
            try {
                put(answer);
            } catch (Throwable th) {
                answer.exceptionally(th);
            }
        }, req.startAsync());
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) {
        handleAsync(answer -> {
            try {
                delete(answer);
            } catch (Throwable th) {
                answer.exceptionally(th);
            }
        }, req.startAsync());
    }

}
