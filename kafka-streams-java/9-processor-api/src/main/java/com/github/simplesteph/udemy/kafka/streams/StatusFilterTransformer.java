package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.Victory;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.github.simplesteph.udemy.kafka.streams.LowLevelProcessorApp.ERROR;

public class StatusFilterTransformer implements ValueTransformer<Victory, Victory> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public Victory transform(Victory value) {
        String status = Optional
                .ofNullable(context.headers().lastHeader("status"))
                .map(header -> new String(header.value(), StandardCharsets.UTF_8))
                .orElse("missing-header");

        return status.equals("invalid") ? ERROR : value;
    }

    @Override
    public void close() {

    }
}
