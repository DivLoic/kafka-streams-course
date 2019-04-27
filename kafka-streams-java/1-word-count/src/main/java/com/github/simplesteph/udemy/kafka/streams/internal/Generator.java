package com.github.simplesteph.udemy.kafka.streams.internal;

import java.util.Arrays;
import java.util.List;

import static org.jeasy.random.util.CollectionUtils.randomElementOf;

public abstract class Generator {

    private static class Character {
        private String name;
        private String country;
        private Character(String name, String country) {
            this.name = name;
            this.country = country;
        }
        public String getName() {
            return name;
        }
    }
    private static Character KEN = new Character("Ken", "US");
    private static Character RYU = new Character("Ryu", "Japan");
    private static Character GEKI = new Character("Geki", "Japan");
    private static Character CHUNLI = new Character("ChunLi", "China");
    private static Character AKUMA = new Character("Akuma", "Japan");
    private static Character SAKURA = new Character("Sakura", "Japan");
    private static Character DHALSIM = new Character("Dhalsim", "India");
    private static Character BLAIR = new Character("Blair", "UK");
    private static Character BLANKA = new Character("BLANKA", "Brazil");

    private static List<Character> STREET_FIGHTER_CAST =
            Arrays.asList(RYU, KEN, CHUNLI, GEKI, AKUMA, SAKURA, DHALSIM, BLAIR, BLANKA);

    protected static String nextGame() {
        Character player1 = randomElementOf(STREET_FIGHTER_CAST);
        Character player2 = randomElementOf(STREET_FIGHTER_CAST);

        return String.format("%s vs %s", player1.getName(), player2.getName());
    }
}
