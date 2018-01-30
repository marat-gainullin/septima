package com.septima.application.endpoint;

import com.septima.application.AsyncEndPoint;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.Map;

public class LogoutEndPoint extends AsyncEndPoint {

    @Override
    public void get(Answer answer) {
        HttpServletRequest request = answer.getRequest();
        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
            answer.withJsonObject(Map.of("description", "User session invalidated"));
        } else {
            answer.erroneous("No logged in user");
        }
    }
}
