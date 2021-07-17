package com.septima.application;

import com.septima.application.endpoint.Answer;
import com.septima.application.endpoint.HttpEndPoint;
import com.septima.application.exceptions.NoImplementationException;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AsyncEndPoint extends HttpServlet implements HttpEndPoint {

    private void handleAsync(Consumer<Answer> aHandler, AsyncContext aContext) {
        aHandler.accept(new Answer(aContext));
    }

    /**
     * This method is final because of security risks.
     * Servlet container may expose security-sensitive information to a client through
     * an exception if it will be thrown during the servlet initialization. To avoid using of
     * complex techniques of #sendError suppressing, override the {@link #prepare()} method.
     * It is wrapped in safe try/catch with secure error handling and without swallowing of the exception.
     */
    @Override
    public final void init() {
        try {
            prepare();
        } catch (Throwable th) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, "Problem while endpoint init", th);
            throw new IllegalStateException("Internal problems. Look for a support, please");
        }
    }

    /**
     * Override this method from descendants code instead of {@link #init()}, due to security risks.
     * Servlet container may expose security-sensitive information to a client through
     * an exception if it will throw during the servlet initialization. To avoid using of
     * complex techniques of #sendError suppressing, override this method.
     * It is wrapped in safe try/catch with secure error handling and without swallowing of the exception.
     * @throws Exception This method is wrapped into try/catch block of general purpose.
     */
    protected void prepare() throws Exception {
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
