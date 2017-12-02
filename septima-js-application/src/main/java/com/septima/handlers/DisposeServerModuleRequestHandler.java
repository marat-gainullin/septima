/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.handlers;

import com.septima.RequestHandler;
import com.septima.client.threetier.requests.DisposeServerModuleRequest;
import com.septima.SeptimaApplication;
import com.septima.Session;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pk
 */
public class DisposeServerModuleRequestHandler extends RequestHandler<DisposeServerModuleRequest, DisposeServerModuleRequest.Response> {

    public DisposeServerModuleRequestHandler(SeptimaApplication aServerCore, DisposeServerModuleRequest aRequest) {
        super(aServerCore, aRequest);
    }

    @Override
    public void handle(Session aSession, Consumer<DisposeServerModuleRequest.Response> onSuccess, Consumer<Exception> onFailure) {
        Logger.getLogger(DisposeServerModuleRequestHandler.class.getName()).log(Level.FINE, "Disposing server module {0}", getRequest().getModuleName());
        try {
            aSession.unregisterModule(getRequest().getModuleName());
            if (onSuccess != null) {
                onSuccess.accept(new DisposeServerModuleRequest.Response());
            }
        } catch (Exception ex) {
            if (onFailure != null) {
                onFailure.accept(ex);
            }
        }
    }
}
