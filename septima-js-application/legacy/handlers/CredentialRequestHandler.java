/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.handlers;

import com.septima.client.threetier.requests.CredentialRequest;
import com.septima.script.Scripts;
import com.septima.SeptimaApplication;
import com.septima.RequestHandler;
import com.septima.Session;
import java.util.function.Consumer;

/**
 *
 * @author mg
 */
public class CredentialRequestHandler extends RequestHandler<CredentialRequest, CredentialRequest.Response> {

    public CredentialRequestHandler(SeptimaApplication aServerCore, CredentialRequest aRequest) {
        super(aServerCore, aRequest);
    }

    @Override
    public void handle(Session aSession, Consumer<CredentialRequest.Response> onSuccess, Consumer<Exception> onFailure) {
        if (onSuccess != null) {
            onSuccess.accept(new CredentialRequest.Response(((PlatypusPrincipal) Scripts.getContext().getPrincipal()).getName()));
        }
    }
}
