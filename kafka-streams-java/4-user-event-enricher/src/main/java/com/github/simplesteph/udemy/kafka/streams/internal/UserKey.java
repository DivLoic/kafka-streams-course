package com.github.simplesteph.udemy.kafka.streams.internal;

public class UserKey {

    private String login;

    public UserKey(String login) {
        this.login = login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getLogin() {
        return login;
    }
}
