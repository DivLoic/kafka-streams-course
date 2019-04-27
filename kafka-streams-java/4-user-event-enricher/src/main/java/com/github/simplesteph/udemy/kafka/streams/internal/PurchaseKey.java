package com.github.simplesteph.udemy.kafka.streams.internal;


import com.fasterxml.jackson.annotation.JsonProperty;

public class PurchaseKey {

    private String id;
    private @JsonProperty("client_id") UserKey clientId;

    public PurchaseKey(String id, UserKey clientId) {
        this.id = id;
        this.clientId = clientId;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setClientId(UserKey clientId) {
        this.clientId = clientId;
    }

    public String getId() {
        return id;
    }

    public UserKey getClientId() {
        return clientId;
    }
}
