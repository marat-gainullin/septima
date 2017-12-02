/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.http;

import com.septima.script.Scripts;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import jdk.nashorn.api.scripting.JSObject;

/**
 *
 * @author mg
 */
public class RequestPrincipal extends PlatypusPrincipal {

    protected HttpServletRequest request;

    public RequestPrincipal(String aUserName, String aDataContext, HttpServletRequest aServletRequest) {
        super(aUserName, aDataContext, Collections.emptySet(), null);
        request = aServletRequest;
    }

    @Override
    public boolean hasRole(String aRole) {
        return request.isUserInRole(aRole);
    }

    @Override
    public void logout(JSObject aOnSuccess, JSObject aOnFailure) throws Exception {
        request.logout();
        request.getSession().invalidate();
        if (aOnSuccess != null) {
            // async style
            Scripts.getSpace().process(() -> {
                aOnSuccess.call(null, new Object[]{});
            });
        } else {
            // sync style
        }
    }
}
