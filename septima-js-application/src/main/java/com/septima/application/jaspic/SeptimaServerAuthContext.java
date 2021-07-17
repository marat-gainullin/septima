package com.septima.application.jaspic;

import javax.security.auth.Subject;
import javax.security.auth.message.AuthException;
import javax.security.auth.message.AuthStatus;
import javax.security.auth.message.MessageInfo;
import javax.security.auth.message.config.ServerAuthContext;
import javax.security.auth.message.module.ServerAuthModule;
import javax.servlet.http.HttpServletRequest;
import java.util.List;

public class SeptimaServerAuthContext implements ServerAuthContext {

    private static final String RESPONSIBLE_MODULE_AT = "responsibleModuleAt";
    private final List<ServerAuthModule> modules;

    public SeptimaServerAuthContext(List<ServerAuthModule> modules) {
        this.modules = modules;
    }

    @SuppressWarnings("unchecked")
    @Override
    public AuthStatus validateRequest(MessageInfo messageInfo, Subject clientSubject,
                                      Subject serviceSubject) throws AuthException {
        for (int i = 0; i < modules.size(); i++) {
            ServerAuthModule module = modules.get(i);
            AuthStatus status = module.validateRequest(messageInfo, clientSubject, serviceSubject);
            if (status != AuthStatus.SEND_FAILURE) { // AuthStatus.FAILURE is not supported here since it is intended for use only with client-side authentication
                messageInfo.getMap().put(RESPONSIBLE_MODULE_AT, i);
                return status;
            }
        }
        messageInfo.getMap().remove(RESPONSIBLE_MODULE_AT);
        return AuthStatus.SEND_FAILURE;
    }


    @Override
    public AuthStatus secureResponse(MessageInfo messageInfo, Subject serviceSubject)
            throws AuthException {
        Integer responsibleModuleAt = (Integer) messageInfo.getMap().get(RESPONSIBLE_MODULE_AT);
        if (responsibleModuleAt != null) {
            messageInfo.getMap().remove(RESPONSIBLE_MODULE_AT);
            if (responsibleModuleAt >= 0 && responsibleModuleAt < modules.size()) {
                return modules.get(responsibleModuleAt).secureResponse(messageInfo, serviceSubject);
            } else {
                return AuthStatus.SUCCESS;
            }
        } else {
            return AuthStatus.SUCCESS;
        }
    }


    @Override
    public void cleanSubject(MessageInfo messageInfo, Subject subject) throws AuthException {
        for (ServerAuthModule module : modules) {
            module.cleanSubject(messageInfo, subject);
        }
    }
}
