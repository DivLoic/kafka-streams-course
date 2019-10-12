package com.github.simplesteph.udemy.kafka.streams

import com.github.simplesteph.Victory
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import java.nio.charset.StandardCharsets

import com.github.simplesteph.udemy.kafka.streams.LowLevelProcessorApp.VictoryError


class StatusFilterTransformer extends ValueTransformer[Victory, Victory] {

  private var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(value: Victory): Victory = {
    val status = Option(context.headers().lastHeader("status"))
      .map(header => new String(header.value(), StandardCharsets.UTF_8))
      .getOrElse("missing-header")

    if (status == "invalid") VictoryError else value
  }

  override def close(): Unit = {}
}
