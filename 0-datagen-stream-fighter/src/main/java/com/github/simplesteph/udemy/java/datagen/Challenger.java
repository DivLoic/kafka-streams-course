package com.github.simplesteph.udemy.java.datagen;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Challenger {

    private String login;
    private Character character;

    public Challenger(String login, Character character) {
        this.login = login;
        this.character = character;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public Character getCharacter() {
        return character;
    }

    public void setCharacter(Character character) {
        this.character = character;
    }

    public ObjectNode toJson() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("login", login);
        node.put("character", character.getName());
        return node;
    }
}
