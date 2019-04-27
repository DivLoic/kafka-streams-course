package com.github.simplesteph.udemy.java.datagen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArcadeContestEOSTest {

    @Test
    public void newRandomTransactionsTest() throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        // given
        Challenger john = new Challenger("john", ArcadeContestEOS.RYU);

        // when
        ProducerRecord<String, String> record = ArcadeContestEOS.newRandomImpact(john);

        String key = record.key();
        String value = record.value();

        // then
        assertEquals("john", key);

        JsonNode node = mapper.readTree(value);
        assertEquals("john", node.get("challenger").get("login").asText());
        assertTrue("Impact should be less than 30", node.get("damage").asInt() < 30);
        assertTrue(Arrays.asList("X", "O").contains(node.get("key").asText()));

        System.out.println(value);

    }

}
