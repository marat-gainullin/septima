/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.login;

import com.septima.script.Scripts;
import com.septima.util.IdGenerator;
import jdk.nashorn.api.scripting.JSObject;

/**
 *
 * @author mg
 */
public class SystemPrincipal implements Principal {

    public SystemPrincipal() {
        super("system-" + IdGenerator.genId(), null, null, null);
    }

    @Override
    public boolean hasRole(String aRole) {
        return true;
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
