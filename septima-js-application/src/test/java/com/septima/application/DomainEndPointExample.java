package com.septima.application;

import com.septima.application.endpoint.Answer;

public class DomainEndPointExample extends AsyncEndPoint {

    @Override
    public void get(Answer answer) {
        Scope.global(Subscription::new, "subscriptions", s -> "junior");
        Scope.session(Profile::new, "terminal.profile", p -> "guru", answer)
                //      .thenCompose(Scope::bind)
                .thenAccept(c -> answer.ok());
    }

    private static class Profile {
    }

    private static class Subscription {
    }

}
