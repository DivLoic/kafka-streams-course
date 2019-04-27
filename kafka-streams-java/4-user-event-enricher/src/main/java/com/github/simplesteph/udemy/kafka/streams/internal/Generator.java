package com.github.simplesteph.udemy.kafka.streams.internal;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import static org.jeasy.random.util.CollectionUtils.randomElementOf;

public class Generator {

    private static Faker faker = new Faker();

    private static Random random = new Random();

    private static EasyRandomParameters parameters = new EasyRandomParameters()
            .seed(60L)
            .objectPoolSize(30)
            .ignoreRandomizationErrors(true)
            .randomize(User.class, () -> {
                Name name = faker.name();
                String firstName = name.firstName();
                String lastName = name.lastName();
                String login = (firstName.toCharArray()[0] + lastName).toLowerCase() + random.nextInt(10);

                return new User(login, firstName, lastName, "NO-EMAIL");
            });

    private static EasyRandom easyRandom = new EasyRandom(parameters);

    protected static Boolean nextIsTwoPlayer() {
        return easyRandom.nextBoolean();
    }

    protected static String nextPurchaseId() {
        return UUID.randomUUID().toString().substring(0, 7);
    }

    protected static Game nextGame() {
        return randomElementOf(Arrays.asList(Game.values()));
    }

    protected static User nextUser(ArrayList<User> users) {
        boolean isKnownUser = easyRandom.nextBoolean();
        return isKnownUser ? randomElementOf(users) : easyRandom.nextObject(User.class);
    }

    protected static Purchase nextPurchase(User user) {
        return new Purchase(user.getLogin(), nextGame(), nextIsTwoPlayer());
    }
}
