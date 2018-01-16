/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.http;

import com.septima.Session;
import com.septima.Sessions;
import static com.septima.http.SeptimaServlet.PLATYPUS_SESSION_ID_ATTR_NAME;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

/**
 *
 * @author mg
 */
public class SessionsSynchronizer implements HttpSessionListener {

    @Override
    public void sessionDestroyed(HttpSessionEvent se) {
        try {
            String pSessionId = (String) se.getSession().getAttribute(PLATYPUS_SESSION_ID_ATTR_NAME);
            if (pSessionId != null) {
                Session removed = Sessions.Singleton.instance.remove(pSessionId);
                if (removed != null) {
                    Logger.getLogger(SessionsSynchronizer.class.getName()).log(Level.INFO, "Platypus session closed. Session id: {0}", removed.getId());
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(SessionsSynchronizer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void sessionCreated(HttpSessionEvent se) {
        // no op. Scripts.Space is appended transform session by servlet code, due transform parallel and sessions replication problems.
    }
}