package com.github.simplesteph.udemy.java.datagen;


import com.fasterxml.jackson.annotation.JsonProperty;

public class Purchase {

    private String user;
    private Game game;
    private @JsonProperty("two_player") Boolean twoPlayer;

    public Purchase(String user, Game game, Boolean twoPlayer) {
        this.user = user;
        this.game = game;
        this.twoPlayer = twoPlayer;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Game getGame() {
        return game;
    }

    public void setGame(Game game) {
        this.game = game;
    }

    public Boolean getTwoPlayer() {
        return twoPlayer;
    }

    public void setTwoPlayer(Boolean twoPlayer) {
        this.twoPlayer = twoPlayer;
    }
}
