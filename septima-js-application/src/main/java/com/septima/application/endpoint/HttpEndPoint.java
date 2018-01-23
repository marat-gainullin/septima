package com.septima.application.endpoint;

public interface HttpEndPoint {
    void get(Answer answer);

    void post(Answer answer);

    void put(Answer answer);

    void delete(Answer answer);
}
