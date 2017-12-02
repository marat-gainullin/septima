/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.handlers;

import com.septima.RequestHandler;
import com.septima.client.threetier.requests.LogoutRequest;
import com.septima.SeptimaApplication;
import com.septima.Session;
import java.util.function.Consumer;

/**
 *
 * @author pk
 */
public class LogoutRequestHandler extends RequestHandler<LogoutRequest, LogoutRequest.Response> {

    public LogoutRequestHandler(SeptimaApplication aServer, LogoutRequest aRequest) {
        super(aServer, aRequest);
    }

    @Override
    public void handle(Session aSession, Consumer<LogoutRequest.Response> onSuccess, Consumer<Exception> onFailure) {
        getServerCore().getSessions().remove(aSession.getId());
        if (onSuccess != null) {
            onSuccess.accept(new LogoutRequest.Response());
        }
    }
}
