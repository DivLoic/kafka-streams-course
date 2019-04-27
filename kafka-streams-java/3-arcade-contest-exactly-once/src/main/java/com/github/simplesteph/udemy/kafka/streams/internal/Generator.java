package com.github.simplesteph.udemy.kafka.streams.internal;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class Generator {

    public static class Character {

        private String name;
        private String country;

        public Character(String name, String country) {
            this.name = name;
            this.country = country;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public ObjectNode toJson() {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("name", name);
            node.put("country", country);
            return node;
        }
    }

    public static class Challenger {

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

}
