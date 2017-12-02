/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.login;

import com.septima.script.Scripts;
import com.septima.util.IdGenerator;

import java.util.Collections;
import jdk.nashorn.api.scripting.JSObject;

/**
 *
 * @author vv
 */
public class AnonymousPrincipal implements Principal {

    public AnonymousPrincipal() {
        this("anonymous-" + IdGenerator.genId());
    }

    public AnonymousPrincipal(String aName) {
        super(aName, null, Collections.emptySet(), null);
    }

    @Override
    public boolean hasRole(String aRole) {
        return false;
    }

    @Override
    public void logout(JSObject aOnSuccess, JSObject aOnFailure) throws Exception {
        if (aOnSuccess != null) {
            // async style
            Scripts.getSpace().process(() -> {
                aOnSuccess.call(null, new Object[]{});
            });
        }
        // sync style
    }
}
