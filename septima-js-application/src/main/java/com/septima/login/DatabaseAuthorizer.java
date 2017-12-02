/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.login;

import com.septima.SeptimaApplication;
import com.septima.client.DatabasesClient;
import com.septima.login.AnonymousPrincipal;
import com.septima.login.MD5Generator;
import com.septima.script.Scripts;
import com.septima.util.IdGenerator;
import java.security.AccessControlException;
import java.util.function.Consumer;
import javax.security.auth.AuthPermission;

/**
 *
 * @author mg
 */
public class DatabaseAuthorizer {

    public static final String LOGIN_INCORRECT_MSG = "Bad user name or password";
    public static final String CREDENTIALS_MISSING_MSG = "User name and password are required while anonymous access is disabled.";

    public static void authorize(SeptimaApplication application, String aUserName, String aPassword, Scripts.Space aSpace, Consumer<PlatypusPrincipal> onSuccess, Consumer<Exception> onFailure) {
        try {
            if (aUserName != null && !aUserName.isEmpty()) {
                String passwordMd5 = MD5Generator.generate(aPassword != null ? aPassword : "");
                DatabasesClient.credentialsToPrincipalWithBasicAuthentication(application.getDatabases(), aUserName, passwordMd5, aSpace, (PlatypusPrincipal principal) -> {
                    if (principal != null) {
                        onSuccess.accept(principal);
                    } else {
                        onFailure.accept(new AccessControlException(LOGIN_INCORRECT_MSG, new AuthPermission("*")));
                    }
                }, onFailure);
            } else {
                PlatypusPrincipal principal = new AnonymousPrincipal("anonymous-" + IdGenerator.genStringId());
                onSuccess.accept(principal);
            }
        } catch (Exception ex) {
            onFailure.accept(ex);
        }
    }
}
