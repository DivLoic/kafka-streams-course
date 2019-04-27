package com.github.simplesteph.udemy.kafka.streams.internal;

import com.github.simplesteph.Character;
import com.github.simplesteph.Game;
import com.github.simplesteph.Player;
import com.github.simplesteph.Victory;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;

import java.time.Instant;
import java.util.*;

import static org.jeasy.random.util.CollectionUtils.randomElementOf;

public abstract class Generator {

    private static final ArrayList<Character> sfCharacters = new ArrayList<>(Arrays.asList(
            new Character("Ken", "US"),
            new Character("Ryu", "Japan"),
            new Character("Geki", "Japan"),
            new Character("Chun-Li", "China"),
            new Character("Akuma", "Japan"),
            new Character("Sakura", "Japan"),
            new Character("Dhalsim", "India"),
            new Character("Blair", "UK")
    ));

    private static final ArrayList<Character> takkenCharacters = new ArrayList<>(Arrays.asList(
            new Character("Jin", "Japan"),
            new Character("Asuka", "Japan"),
            new Character("Emilie", "Monaco"),
            new Character("Kazuya", "Japan")
    ));

    private static final ArrayList<Character> kofCharacters = new ArrayList<>(Arrays.asList(
            new Character("Mai", "Japan"),
            new Character("Ramon", "Mexico"),
            new Character("Nelson", "Brazil"),
            new Character("Vanessa", "France")
    ));

    private static final ArrayList<Character> scCharacters = new ArrayList<>(Arrays.asList(
            new Character("Kilik", "China"),
            new Character("Ivy", "UK"),
            new Character("Siegfried", "HRE"),
            new Character("Nightmare", "X")
    ));

    private static final ArrayList<Character> smCharacters = new ArrayList<>(Arrays.asList(
            new Character("Galford", "US"),
            new Character("Charlotte", "France"),
            new Character("Haohmaru", "Japan"),
            new Character("Ukyo Tachibana", "Japan")
    ));

    private static final HashMap<Game, List<Character>> CHARACTERS_MAP = new HashMap<Game, List<Character>>() {{
        put(Game.KingOfFighters, kofCharacters);
        put(Game.SamuraiShodown, smCharacters);
        put(Game.StreetFighter, sfCharacters);
        put(Game.SoulCalibur, scCharacters);
        put(Game.Takken, takkenCharacters);
    }};

    private static EasyRandomParameters parameters = new EasyRandomParameters()
            .seed(60L)
            .objectPoolSize(30)
            .ignoreRandomizationErrors(true)
            .randomize(Victory.class, () -> {
                Game game = nextGame();
                Character character = nextCharacter(game);
                String termId = Integer.toString(new Random().nextInt(3));
                return new Victory(
                        game,
                        termId,
                        character,
                        Instant.now(),
                        randomElementOf(Arrays.asList(Player.values()))
                );
            });

    private static EasyRandom easyRandom = new EasyRandom(parameters);

    private static Game nextGame() {
        return randomElementOf(Arrays.asList(Game.StreetFighter, Game.Takken, Game.KingOfFighters));
    }

    private static Character nextCharacter(Game game) {
        return randomElementOf(CHARACTERS_MAP.get(game));
    }

    protected static Optional<Victory> nextVictory() {
        return Optional.ofNullable(easyRandom.nextObject(Victory.class));
    }

    protected static int nextDelay() {
        return 1 + easyRandom.nextInt(5);
    }
}

