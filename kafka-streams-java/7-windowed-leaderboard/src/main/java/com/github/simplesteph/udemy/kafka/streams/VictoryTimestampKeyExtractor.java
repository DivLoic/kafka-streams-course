package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.Victory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class VictoryTimestampKeyExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        long newTimestamp;
        try {
            newTimestamp = ((Victory) record.value()).getTimestamp().toEpochMilli();
        } catch (Throwable e) {
            newTimestamp = previousTimestamp;
        }
        return newTimestamp;
    }
}
