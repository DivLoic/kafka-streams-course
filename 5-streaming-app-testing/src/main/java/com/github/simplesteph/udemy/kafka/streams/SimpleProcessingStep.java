package com.github.simplesteph.udemy.kafka.streams;


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleProcessingStep {

    public static KeyValueMapper<String, String, KeyValue<Integer, String>> formatKeyValue =
            (key, value) -> new KeyValue<>(Integer.parseInt(key),  value.split(",")[1].toLowerCase());

    public static ValueMapper<String, String> getMachineId = value -> {
        Matcher match = Pattern.compile("^terminal-(\\d+),.*$").matcher(value);
        return match.find() ? match.group(1) : null;
    };

    public static Predicate<String, String> isTopCharacter =
            (key, value) -> Arrays.asList("ryu", "ken", "chunli").contains(value);
}
